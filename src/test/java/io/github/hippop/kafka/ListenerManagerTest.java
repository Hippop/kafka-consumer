package io.github.hippop.kafka;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BooleanSupplier;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.errors.TimeoutException;
import org.junit.jupiter.api.Test;

import static io.github.hippop.kafka.ListenerSnapshot.CommitState;
import static io.github.hippop.kafka.ListenerSnapshot.State;
import static org.junit.jupiter.api.Assertions.*;

class ListenerManagerTest {
    static final Map<String, Object> CONFIG = Map.of("bootstrap.servers", "localhost:9092", "auto.offset.reset", "earliest");
    static final ListenerOptions OPTIONS = new ListenerOptions(Duration.ofMillis(5), Duration.ofMillis(30),
            Duration.ofMillis(100), Duration.ofSeconds(3), Duration.ofMillis(600));

    static ListenerManager manager(ConsumerFactory factory) { return new ListenerManager(CONFIG, OPTIONS, factory); }
    static void register(ListenerManager manager, MessageListener listener) {
        manager.register("events", List.of("events"), "tests", listener);
    }

    @Test void registrationIsLazyAndValidationIsTransactional() {
        AtomicInteger created = new AtomicInteger();
        try (ListenerManager manager = manager(config -> { created.incrementAndGet(); return new ScriptedConsumer(); })) {
            manager.register(new Annotated());
            assertEquals(0, created.get());
            assertEquals(State.NEW, manager.status("annotated").state());
            assertThrows(IllegalArgumentException.class, () -> manager.register(new InvalidAnnotated()));
            assertThrows(IllegalArgumentException.class, () -> manager.register(new Annotated()));
            assertThrows(IllegalArgumentException.class, () -> manager.start("missing"));
            assertThrows(IllegalArgumentException.class, () -> manager.pause("annotated", " "));
            assertEquals(0, created.get());
        }
    }

    @Test void annotatedCheckedBusinessExceptionIsUnwrappedSkippedAndCommitted() throws Exception {
        ScriptedConsumer consumer = new ScriptedConsumer();
        consumer.initialRecords(2);
        ProcessingAnnotated listener = new ProcessingAnnotated();
        try (ListenerManager manager = manager(config -> consumer)) {
            manager.register(listener);
            manager.startAll();
            await(() -> listener.handled.get() == 2 && consumer.commits.size() == 1);
            assertEquals(2, consumer.commits.get(0).offsets().get(ScriptedConsumer.TP).offset());
        }
    }

    @Test void overlappingPauseOwnersAreIdempotentAndScoped() throws Exception {
        ScriptedConsumer consumer = new ScriptedConsumer();
        AtomicInteger created = new AtomicInteger();
        try (ListenerManager manager = manager(config -> { created.incrementAndGet(); return consumer; })) {
            register(manager, ignored -> {});
            manager.pauseAll("same");
            manager.pause("events", "same");
            manager.pause("events", "same");
            manager.start("events");
            await(() -> manager.status("events").state() == State.PAUSED);
            assertEquals(0, created.get());
            manager.resumeAll("same");
            assertEquals(1, manager.status("events").localPauseOwners().size());
            assertThrows(UnsupportedOperationException.class,
                    () -> manager.status("events").localPauseOwners().add("bad"));
            manager.resume("events", "unknown");
            assertEquals(0, created.get());
            manager.resume("events", "same");
            manager.resume("events", "same");
            await(() -> created.get() == 1 && consumer.polls.get() > 2);
            manager.pauseAll("global");
            await(() -> manager.status("events").state() == State.PAUSED);
            manager.startAll();
            assertEquals(1, created.get());
            manager.resumeAll("global");
            await(() -> manager.status("events").state() == State.RUNNING);
            assertEquals(1, created.get());
            assertEquals(1, consumer.subscriptions.get());
        }
    }

    @Test void resumeWithoutStartDoesNotCreateConsumer() {
        AtomicInteger created = new AtomicInteger();
        try (ListenerManager manager = manager(config -> { created.incrementAndGet(); return new ScriptedConsumer(); })) {
            register(manager, ignored -> {});
            manager.pause("events", "owner");
            manager.resume("events", "owner");
            assertEquals(0, created.get());
            assertFalse(manager.status("events").startedRequested());
        }
    }

    @Test void concurrentStartCreatesExactlyOneConsumerAndFreezesRegistration() throws Exception {
        ScriptedConsumer consumer = new ScriptedConsumer();
        AtomicInteger created = new AtomicInteger();
        try (ListenerManager manager = manager(config -> { created.incrementAndGet(); return consumer; })) {
            register(manager, ignored -> {});
            var callers = Executors.newFixedThreadPool(8);
            try {
                var calls = new ArrayList<java.util.concurrent.Callable<Void>>();
                for (int i = 0; i < 80; i++) calls.add(() -> { manager.start("events"); return null; });
                for (var future : callers.invokeAll(calls)) future.get();
            } finally { callers.shutdownNow(); }
            await(() -> consumer.polls.get() > 3);
            assertEquals(1, created.get());
            assertThrows(IllegalStateException.class, () -> manager.register(new Annotated()));
            assertEquals(1, consumer.operationThreads.size());
        }
    }

    @Test void pauseDrainsWholeBatchSkipsBusinessExceptionAndReusesConsumer() throws Exception {
        ScriptedConsumer consumer = new ScriptedConsumer();
        consumer.initialRecords(10);
        AtomicInteger handled = new AtomicInteger();
        AtomicInteger created = new AtomicInteger();
        try (ListenerManager manager = manager(config -> { created.incrementAndGet(); return consumer; })) {
            register(manager, record -> {
                handled.incrementAndGet();
                if (record.offset() == 2) {
                    manager.pause("events", "A");
                    manager.pause("events", "B");
                }
                if (record.offset() == 4) throw new IllegalStateException("business failure");
            });
            manager.start("events");
            await(() -> manager.status("events").state() == State.PAUSED && handled.get() == 10);
            assertEquals(10, consumer.commits.get(0).offsets().get(ScriptedConsumer.TP).offset());
            int polls = consumer.polls.get();
            consumer.addLater(10);
            manager.resume("events", "A");
            await(() -> consumer.polls.get() > polls + 10);
            assertEquals(10, handled.get());
            assertTrue(consumer.lastPaused.contains(ScriptedConsumer.TP));
            manager.resume("events", "B");
            await(() -> handled.get() == 11 && consumer.commits.size() == 2);
            assertEquals(1, created.get());
            assertEquals(1, consumer.subscriptions.get());
        }
    }

    @Test void asyncRequestReturnDoesNotConfirmProgressOrOpenNextBatch() throws Exception {
        ScriptedConsumer consumer = new ScriptedConsumer();
        consumer.automaticAck.set(false);
        consumer.initialRecords(2);
        AtomicInteger handled = new AtomicInteger();
        try (ListenerManager manager = manager(config -> consumer)) {
            register(manager, ignored -> handled.incrementAndGet());
            manager.startAll();
            await(() -> consumer.commits.size() == 1);
            consumer.addLater(2);
            int polls = consumer.polls.get();
            await(() -> consumer.polls.get() > polls + 10);
            assertEquals(2, handled.get());
            assertEquals(CommitState.IN_FLIGHT, manager.status("events").commitState());
            consumer.acknowledge(null);
            await(() -> handled.get() == 3 && consumer.commits.size() == 2);
            consumer.acknowledge(null);
            await(() -> manager.status("events").commitState() == CommitState.NONE);
        }
    }

    @Test void callbackFailureRetriesWithoutReexecutingBusinessAndIgnoresOldCallback() throws Exception {
        ScriptedConsumer consumer = new ScriptedConsumer();
        consumer.automaticAck.set(false);
        consumer.initialRecords(3);
        AtomicInteger handled = new AtomicInteger();
        try (ListenerManager manager = manager(config -> consumer)) {
            register(manager, ignored -> handled.incrementAndGet());
            manager.startAll();
            await(() -> consumer.commits.size() == 1);
            var old = consumer.commits.get(0);
            consumer.acknowledge(new TimeoutException("commit failed"));
            await(() -> consumer.commits.size() == 2);
            assertEquals(3, handled.get());
            assertEquals(old.offsets(), consumer.commits.get(1).offsets());
            old.callback().onComplete(old.offsets(), null);
            int polls = consumer.polls.get();
            await(() -> consumer.polls.get() > polls + 5);
            assertEquals(CommitState.IN_FLIGHT, manager.status("events").commitState());
            consumer.acknowledge(null);
            await(() -> manager.status("events").commitState() == CommitState.NONE);
        }
    }

    @Test void synchronousAsyncSubmissionFailureIsRetried() throws Exception {
        ScriptedConsumer consumer = new ScriptedConsumer();
        consumer.initialRecords(2);
        consumer.commitFailure = new TimeoutException("submission failed");
        AtomicInteger handled = new AtomicInteger();
        try (ListenerManager manager = manager(config -> consumer)) {
            register(manager, ignored -> handled.incrementAndGet());
            manager.startAll();
            await(() -> consumer.commitInvocations.get() == 2
                    && manager.status("events").commitState() == CommitState.NONE);
            assertEquals(2, handled.get());
            assertEquals(1, consumer.commits.size());
        }
    }

    @Test void creationFailureBacksOffThenRecoversWithForcedConsumerConfiguration() throws Exception {
        AtomicInteger attempts = new AtomicInteger();
        List<Long> times = new CopyOnWriteArrayList<>();
        ScriptedConsumer consumer = new ScriptedConsumer();
        try (ListenerManager manager = manager(config -> {
            times.add(System.nanoTime());
            assertEquals(false, config.get("enable.auto.commit"));
            assertEquals("tests", config.get("group.id"));
            if (attempts.incrementAndGet() < 3) throw new IllegalStateException("create failed");
            return consumer;
        })) {
            register(manager, ignored -> {});
            manager.startAll();
            await(() -> consumer.polls.get() > 3 && manager.status("events").consecutiveFailures() == 0);
            assertEquals(3, attempts.get());
            assertTrue(times.get(1) - times.get(0) >= Duration.ofMillis(25).toNanos());
            assertTrue(times.get(2) - times.get(1) >= Duration.ofMillis(50).toNanos());
        }
    }

    @Test void subscriptionFailureClosesPartialConsumerAndRecreates() throws Exception {
        ScriptedConsumer first = new ScriptedConsumer();
        first.subscribeFailure = new IllegalStateException("subscribe failed");
        ScriptedConsumer second = new ScriptedConsumer();
        AtomicInteger attempts = new AtomicInteger();
        try (ListenerManager manager = manager(config -> attempts.incrementAndGet() == 1 ? first : second)) {
            register(manager, ignored -> {});
            manager.startAll();
            await(() -> second.polls.get() > 2);
            assertTrue(first.closedFlag.get());
            assertEquals(2, attempts.get());
        }
    }

    @Test void retriablePollFailureRetainsConsumer() throws Exception {
        ScriptedConsumer consumer = new ScriptedConsumer();
        consumer.commands.add(() -> { throw new TimeoutException("temporary poll failure"); });
        AtomicInteger attempts = new AtomicInteger();
        try (ListenerManager manager = manager(config -> { attempts.incrementAndGet(); return consumer; })) {
            register(manager, ignored -> {});
            manager.startAll();
            await(() -> consumer.polls.get() > 4 && manager.status("events").consecutiveFailures() == 0);
            assertEquals(1, attempts.get());
            assertFalse(consumer.closedFlag.get());
        }
    }

    @Test void pauseDuringCreationPreventsInitialConsumption() throws Exception {
        CountDownLatch creating = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        ScriptedConsumer consumer = new ScriptedConsumer();
        try (ListenerManager manager = manager(config -> {
            creating.countDown();
            assertTrue(release.await(2, TimeUnit.SECONDS));
            return consumer;
        })) {
            register(manager, ignored -> fail("must not consume"));
            manager.startAll();
            assertTrue(creating.await(2, TimeUnit.SECONDS));
            manager.pauseAll("blocked");
            release.countDown();
            await(() -> manager.status("events").state() == State.PAUSED);
            assertEquals(0, consumer.subscriptions.get());
            assertEquals(0, consumer.polls.get());
        } finally { release.countDown(); }
    }

    @Test void rebalanceInvalidatesOldBatchAndLateBusinessResult() throws Exception {
        ScriptedConsumer consumer = new ScriptedConsumer();
        consumer.initialRecords(3);
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger handled = new AtomicInteger();
        try (ListenerManager manager = manager(config -> consumer)) {
            register(manager, ignored -> {
                if (handled.incrementAndGet() == 1) {
                    entered.countDown();
                    assertTrue(release.await(2, TimeUnit.SECONDS));
                }
            });
            manager.startAll();
            assertTrue(entered.await(2, TimeUnit.SECONDS));
            CountDownLatch reassigned = new CountDownLatch(1);
            consumer.commands.add(() -> {
                consumer.reassign(List.of());
                consumer.reassign(List.of(ScriptedConsumer.TP));
                consumer.addRecord(ScriptedConsumer.record(0));
                reassigned.countDown();
            });
            assertTrue(reassigned.await(2, TimeUnit.SECONDS));
            release.countDown();
            await(() -> handled.get() == 2 && consumer.commits.size() == 1);
            assertEquals(1, consumer.commits.get(0).offsets().get(ScriptedConsumer.TP).offset());
        } finally { release.countDown(); }
    }

    @Test void lostPartitionDoesNotCommitLateResult() throws Exception {
        ScriptedConsumer consumer = new ScriptedConsumer();
        consumer.initialRecords(1);
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        try (ListenerManager manager = manager(config -> consumer)) {
            register(manager, ignored -> { entered.countDown(); assertTrue(release.await(2, TimeUnit.SECONDS)); });
            manager.startAll();
            assertTrue(entered.await(2, TimeUnit.SECONDS));
            CountDownLatch lost = new CountDownLatch(1);
            consumer.commands.add(() -> { consumer.loseAll(); lost.countDown(); });
            assertTrue(lost.await(2, TimeUnit.SECONDS));
            release.countDown();
            await(() -> manager.status("events").batchId() == 0);
            assertTrue(consumer.commits.isEmpty());
        } finally { release.countDown(); }
    }

    @Test void fatalPollRebuildDoesNotUseOldBusinessResult() throws Exception {
        ScriptedConsumer first = new ScriptedConsumer();
        ScriptedConsumer second = new ScriptedConsumer();
        first.initialRecords(1);
        second.initialRecords(1);
        AtomicInteger builds = new AtomicInteger();
        AtomicInteger handled = new AtomicInteger();
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        try (ListenerManager manager = manager(config -> builds.incrementAndGet() == 1 ? first : second)) {
            register(manager, ignored -> {
                if (handled.incrementAndGet() == 1) { entered.countDown(); assertTrue(release.await(2, TimeUnit.SECONDS)); }
            });
            manager.startAll();
            assertTrue(entered.await(2, TimeUnit.SECONDS));
            first.commands.add(() -> { throw new IllegalStateException("broken consumer"); });
            await(() -> builds.get() == 2 && second.polls.get() > 2);
            assertEquals(1, handled.get());
            release.countDown();
            await(() -> handled.get() == 2 && second.commits.size() == 1);
            assertTrue(first.commits.isEmpty());
            assertEquals(1, second.commits.get(0).offsets().get(ScriptedConsumer.TP).offset());
        } finally { release.countDown(); }
    }

    @Test void boundedCloseDoesNotInterruptBusinessOrStartRemainingRecords() throws Exception {
        ScriptedConsumer consumer = new ScriptedConsumer();
        consumer.initialRecords(3);
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger handled = new AtomicInteger();
        AtomicReference<Throwable> interruption = new AtomicReference<>();
        ListenerManager manager = manager(config -> consumer);
        try {
            register(manager, ignored -> {
                handled.incrementAndGet();
                entered.countDown();
                try { release.await(); } catch (InterruptedException failure) { interruption.set(failure); }
            });
            manager.startAll();
            assertTrue(entered.await(2, TimeUnit.SECONDS));
            long started = System.nanoTime();
            manager.close();
            assertTrue(System.nanoTime() - started < Duration.ofSeconds(2).toNanos());
            assertThrows(IllegalStateException.class, () -> manager.startAll());
            release.countDown();
            await(consumer.closedFlag::get);
            assertEquals(1, handled.get());
            assertNull(interruption.get());
        } finally { release.countDown(); manager.close(); }
    }

    @Test void fatalBusinessErrorStopsContainer() throws Exception {
        ScriptedConsumer consumer = new ScriptedConsumer();
        consumer.initialRecords(2);
        AtomicInteger handled = new AtomicInteger();
        try (ListenerManager manager = manager(config -> consumer)) {
            register(manager, ignored -> { handled.incrementAndGet(); throw new AssertionError("fatal"); });
            manager.startAll();
            await(() -> manager.status("events").state() == State.FAILED);
            assertEquals(1, handled.get());
            assertTrue(consumer.commits.isEmpty());
            assertThrows(IllegalStateException.class, () -> manager.startAll());
        }
    }

    @Test void pauseWinningAgainstPollReturnKeepsBatchUnadmitted() throws Exception {
        ScriptedConsumer consumer = new ScriptedConsumer();
        consumer.initialRecords(3);
        AtomicInteger handled = new AtomicInteger();
        try (ListenerManager manager = manager(config -> consumer)) {
            register(manager, ignored -> handled.incrementAndGet());
            consumer.beforeFirstRecords = () -> manager.pause("events", "race");
            manager.startAll();
            await(() -> manager.status("events").state() == State.PAUSED
                    && manager.status("events").batchSize() == 3);
            assertEquals(0, handled.get());
            assertTrue(consumer.commits.isEmpty());
            manager.resume("events", "race");
            await(() -> handled.get() == 3 && consumer.commits.size() == 1);
            assertEquals(3, consumer.commits.get(0).offsets().get(ScriptedConsumer.TP).offset());
        }
    }

    @Test void pauseStopsRecreationDuringBackoffAndStartDoesNotOverrideIt() throws Exception {
        ScriptedConsumer consumer = new ScriptedConsumer();
        AtomicInteger attempts = new AtomicInteger();
        ListenerOptions timers = new ListenerOptions(Duration.ofMillis(5), Duration.ofMillis(100),
                Duration.ofMillis(200), Duration.ofSeconds(3), Duration.ofMillis(500));
        try (ListenerManager manager = new ListenerManager(CONFIG, timers, config -> {
            if (attempts.incrementAndGet() == 1) throw new IllegalStateException("create failed");
            return consumer;
        })) {
            register(manager, ignored -> {});
            manager.startAll();
            await(() -> manager.status("events").state() == State.RETRYING);
            manager.pause("events", "blocked");
            manager.startAll();
            Thread.sleep(250);
            assertEquals(1, attempts.get());
            manager.resume("events", "blocked");
            await(() -> consumer.polls.get() > 2);
            assertEquals(2, attempts.get());
        }
    }

    @Test void missingCallbackRetiresInstanceAndLateCallbackCannotAffectReplacement() throws Exception {
        ScriptedConsumer first = new ScriptedConsumer();
        first.automaticAck.set(false);
        first.initialRecords(1);
        ScriptedConsumer second = new ScriptedConsumer();
        second.initialRecords(1);
        AtomicInteger builds = new AtomicInteger();
        AtomicInteger handled = new AtomicInteger();
        ListenerOptions timers = new ListenerOptions(Duration.ofMillis(5), Duration.ofMillis(20),
                Duration.ofMillis(50), Duration.ofMillis(100), Duration.ofMillis(500));
        try (ListenerManager manager = new ListenerManager(CONFIG, timers,
                config -> builds.incrementAndGet() == 1 ? first : second)) {
            register(manager, ignored -> handled.incrementAndGet());
            manager.startAll();
            await(() -> second.commits.size() == 1 && manager.status("events").batchId() == 0);
            assertEquals(2, handled.get());
            assertEquals(2, builds.get());
            assertTrue(first.closedFlag.get());
            ScriptedConsumer.Request stale = first.commits.get(0);
            stale.callback().onComplete(stale.offsets(), new TimeoutException("late old callback"));
            int polls = second.polls.get();
            await(() -> second.polls.get() > polls + 5);
            assertEquals(0, manager.status("events").consecutiveFailures());
            assertEquals(CommitState.NONE, manager.status("events").commitState());
        }
    }

    @Test void namedAndAllControlsWorkAcrossMultipleListeners() throws Exception {
        ScriptedConsumer first = new ScriptedConsumer();
        ScriptedConsumer second = new ScriptedConsumer();
        AtomicInteger created = new AtomicInteger();
        try (ListenerManager manager = manager(config -> {
            created.incrementAndGet();
            return "first-group".equals(config.get("group.id")) ? first : second;
        })) {
            manager.register("first", List.of("events"), "first-group", ignored -> {});
            manager.register("second", List.of("events"), "second-group", ignored -> {});
            manager.start("first");
            await(() -> first.polls.get() > 2);
            assertFalse(manager.status("second").startedRequested());
            manager.pauseAll("global");
            manager.startAll();
            await(() -> manager.status("first").state() == State.PAUSED
                    && manager.status("second").state() == State.PAUSED);
            assertEquals(1, created.get());
            manager.pause("first", "local");
            manager.resumeAll("global");
            await(() -> second.polls.get() > 2);
            assertEquals(State.PAUSED, manager.status("first").state());
            manager.resume("first", "local");
            await(() -> manager.status("first").state() == State.RUNNING);
            assertEquals(2, created.get());
        }
    }

    @Test void defaultBackoffCapsAtThirtyMinutesWithoutOverflow() {
        RetryPolicy retry = new RetryPolicy(Duration.ofSeconds(30), Duration.ofMinutes(30));
        assertEquals(Duration.ofSeconds(30), retry.delay(1));
        assertEquals(Duration.ofMinutes(16), retry.delay(6));
        assertEquals(Duration.ofMinutes(30), retry.delay(7));
        assertEquals(Duration.ofMinutes(30), retry.delay(Integer.MAX_VALUE));
        assertThrows(IllegalArgumentException.class, () -> retry.delay(0));
    }

    static void await(BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
        while (!condition.getAsBoolean() && System.nanoTime() - deadline < 0) {
            if (Thread.interrupted()) throw new InterruptedException();
            LockSupport.parkNanos(Duration.ofMillis(2).toNanos());
        }
        assertTrue(condition.getAsBoolean(), "Condition not met before deadline");
    }

    public static class Annotated {
        @KafkaMessageListener(name = "annotated", topics = "events", groupId = "tests")
        public void handle(ConsumerRecord<byte[], byte[]> record) {}
    }

    public static class InvalidAnnotated {
        @KafkaMessageListener(name = "invalid", topics = "events", groupId = "tests")
        private void handle(ConsumerRecord<byte[], byte[]> record) {}
    }

    public static class ProcessingAnnotated {
        final AtomicInteger handled = new AtomicInteger();
        @KafkaMessageListener(name = "annotated", topics = "events", groupId = "tests")
        public void handle(ConsumerRecord<byte[], byte[]> record) throws java.io.IOException {
            handled.incrementAndGet();
            if (record.offset() == 0) throw new java.io.IOException("expected checked business exception");
        }
    }
}
