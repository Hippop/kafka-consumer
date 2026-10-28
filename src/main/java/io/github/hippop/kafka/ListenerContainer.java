package io.github.hippop.kafka;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.github.hippop.kafka.ListenerSnapshot.CommitState;
import static io.github.hippop.kafka.ListenerSnapshot.State;

/** All fields except controls, the event queue and explicitly volatile fields are IO-thread-owned. */
final class ListenerContainer {
    private static final Logger LOG = LoggerFactory.getLogger(ListenerContainer.class);
    final ListenerManager.Control control;
    private final ListenerManager manager;
    private final ListenerManager.Definition definition;
    private final Map<String, Object> configuration;
    private final ConsumerFactory factory;
    private final ListenerOptions options;
    private final RetryPolicy retry;
    private final ExecutorService worker;
    private final ConcurrentLinkedQueue<Event> events = new ConcurrentLinkedQueue<>();
    private final Map<TopicPartition, Long> ownership = new HashMap<>();
    private final Map<TopicPartition, Progress> dirty = new HashMap<>();
    private volatile Consumer<byte[], byte[]> consumer;
    private volatile Thread thread;
    private volatile ListenerSnapshot view;
    private volatile boolean closing;
    private volatile long closeDeadline;
    private boolean failed;
    private boolean subscribed;
    private long instanceId;
    private long ownershipSequence;
    private long batchSequence;
    private long requestSequence;
    private long pollCount;
    private Batch batch;
    private Batch waitingBatch;
    private TaggedRecord busy;
    private CommitRequest request;
    private long createRetryAt;
    private long pollRetryAt;
    private long commitRetryAt;
    private int failures;
    private String lastPhase;
    private String lastError;
    private String step = "CREATE";

    ListenerContainer(ListenerManager manager, ListenerManager.Definition definition,
                      Map<String, Object> configuration, ConsumerFactory factory, ListenerOptions options) {
        this.manager = manager;
        this.definition = definition;
        this.configuration = configuration;
        this.factory = factory;
        this.options = options;
        retry = new RetryPolicy(options.initialRetry(), options.maxRetry());
        control = manager.new Control();
        worker = Executors.newSingleThreadExecutor(task -> daemon(task, "handler"));
        publish(null);
    }

    // Called with the manager control lock, so concurrent start requests cannot create two runners.
    void checkStartable() {
        if (view.state() == State.FAILED || view.state() == State.CLOSED) {
            throw new IllegalStateException("Listener is terminal: " + definition.name());
        }
    }

    void ensureStarted() {
        if (thread == null) {
            thread = daemon(this::run, "consumer");
            thread.start();
        }
    }

    void signal() {
        Thread runner = thread;
        if (runner != null) LockSupport.unpark(runner);
        Consumer<byte[], byte[]> current = consumer;
        if (current != null) {
            try {
                current.wakeup();
            } catch (IllegalStateException ignored) {
                // A concurrent retirement may have already closed this instance.
            }
        }
    }

    void requestClose(long deadline) {
        closeDeadline = deadline;
        closing = true;
        if (thread == null) {
            worker.shutdown();
            publish(State.CLOSED);
        }
        signal();
    }

    void awaitClose(long deadline) {
        Thread runner = thread;
        if (runner == null || runner == Thread.currentThread()) return;
        try {
            long remaining = deadline - System.nanoTime();
            if (remaining > 0) TimeUnit.NANOSECONDS.timedJoin(runner, remaining);
            if (runner.isAlive()) LOG.warn("Listener {} close deadline reached", definition.name());
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    ListenerSnapshot snapshot() {
        ListenerSnapshot current = view;
        return new ListenerSnapshot(current.name(), current.state(), control.started,
                Set.copyOf(control.owners), Set.copyOf(manager.globalOwners), current.assignedPartitions(),
                current.batchId(), current.batchSize(), current.completedRecords(), current.commitState(),
                current.pollCount(), current.failurePhase(), current.lastError(), current.consecutiveFailures(),
                current.nextRetryAt());
    }

    private Thread daemon(Runnable task, String role) {
        Thread result = new Thread(task, "kafka-listener-" + definition.name() + "-" + role);
        result.setDaemon(true);
        return result;
    }

    private void run() {
        try {
            while (!failed) {
                drainEvents();
                if (failed) break;
                long now = System.nanoTime();
                if (closing && (now - closeDeadline >= 0 || drained())) break;
                try {
                    if (!ready(now)) {
                        publish(null);
                        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(20));
                        continue;
                    }
                    if (request != null && now - request.submittedAt() >= options.commitCallbackTimeout().toNanos()) {
                        throw new CommitCallbackTimeout();
                    }
                    admitWaitingBatch();
                    dispatch(now);
                    finishBatch(now);
                    applyFetchState();
                    if (pollRetryAt == 0 || now - pollRetryAt >= 0) {
                        step = "POLL";
                        ConsumerRecords<byte[], byte[]> records = consumer.poll(pollDuration());
                        pollCount++;
                        pollRetryAt = 0;
                        accept(records);
                        if (batch == null && waitingBatch == null && request == null && dirty.isEmpty()
                                && commitRetryAt == 0) healthy();
                        if (records.isEmpty()) LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
                    } else {
                        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(10));
                    }
                } catch (WakeupException controlSignal) {
                    // Both external control and worker completion may wake the consumer.
                } catch (Exception exception) {
                    boolean retain = "POLL".equals(step) && exception instanceof RetriableException
                            && !(exception instanceof CommitCallbackTimeout);
                    long deadline = failure(exception instanceof CommitCallbackTimeout ? "COMMIT" : step, exception);
                    if (retain) pollRetryAt = deadline;
                    else {
                        retire();
                        createRetryAt = deadline;
                    }
                }
                publish(null);
            }
        } catch (Error fatal) {
            fatal(fatal);
        } finally {
            if (closing && !drained()) LOG.warn("Listener {} closed with an unfinished batch/commit", definition.name());
            retire();
            worker.shutdown();
            publish(failed ? State.FAILED : State.CLOSED);
        }
    }

    private boolean ready(long now) throws Exception {
        if (consumer == null) {
            if (closing || !control.allowed() || (createRetryAt != 0 && now - createRetryAt < 0)) return false;
            step = "CREATE";
            Consumer<byte[], byte[]> created = factory.create(configuration);
            if (created == null) throw new IllegalStateException("ConsumerFactory returned null");
            instanceId++;
            consumer = created;
            subscribed = false;
            createRetryAt = 0;
        }
        if (!subscribed) {
            if (closing || !control.allowed()) return false;
            step = "SUBSCRIBE";
            consumer.subscribe(definition.topics(), rebalanceListener(instanceId));
            subscribed = true;
        }
        return true;
    }

    private void accept(ConsumerRecords<byte[], byte[]> records) {
        if (records.isEmpty()) return;
        if (batch != null || waitingBatch != null || request != null) {
            // Never discard an unexpectedly fetched record then later commit beyond it.
            throw new IllegalStateException("Received a second batch while fetching should be paused");
        }
        long id = ++batchSequence;
        List<TaggedRecord> tagged = new ArrayList<>(records.count());
        for (ConsumerRecord<byte[], byte[]> record : records) {
            TopicPartition partition = new TopicPartition(record.topic(), record.partition());
            Long epoch = ownership.get(partition);
            if (epoch == null) throw new IllegalStateException("Record has no valid partition ownership");
            tagged.add(new TaggedRecord(instanceId, epoch, id, partition, record));
        }
        waitingBatch = new Batch(id, tagged);
        admitWaitingBatch();
        consumer.pause(consumer.assignment());
    }

    private void admitWaitingBatch() {
        synchronized (manager.controlLock) {
            if (batch == null && waitingBatch != null && control.allowed() && !closing) {
                batch = waitingBatch;
                waitingBatch = null;
            }
        }
    }

    private void dispatch(long now) {
        if (batch == null || busy != null || (pollRetryAt != 0 && now - pollRetryAt < 0)) return;
        if (closing && now - closeDeadline >= 0) return;
        while (batch.index < batch.records.size()) {
            TaggedRecord tagged = batch.records.get(batch.index++);
            if (!valid(tagged)) continue;
            busy = tagged;
            worker.execute(() -> {
                Error fatal = null;
                try {
                    definition.listener().onMessage(tagged.record());
                } catch (Exception businessFailure) {
                    LOG.error("Listener {} HANDLE failed; skipping topic={} partition={} offset={}",
                            definition.name(), tagged.record().topic(), tagged.record().partition(),
                            tagged.record().offset(), businessFailure);
                } catch (Error error) {
                    fatal = error;
                } finally {
                    events.add(new HandlerDone(tagged, fatal));
                    signal();
                }
            });
            break;
        }
    }

    private void finishBatch(long now) {
        if (batch == null || busy != null || batch.index < batch.records.size()) return;
        if (request != null) return;
        if (dirty.isEmpty()) {
            batch = null;
            healthy();
        } else if (commitRetryAt == 0 || now - commitRetryAt >= 0) {
            submitCommit();
        }
    }

    private void submitCommit() {
        if (request != null || consumer == null) return;
        Map<TopicPartition, Progress> progress = new LinkedHashMap<>();
        dirty.forEach((partition, value) -> {
            if (valid(partition, instanceId, value.epoch())) progress.put(partition, value);
        });
        if (progress.isEmpty()) return;
        CommitRequest submitted = new CommitRequest(instanceId, ++requestSequence,
                Map.copyOf(progress), System.nanoTime());
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        progress.forEach((partition, value) -> offsets.put(partition, value.offset()));
        request = submitted;
        commitRetryAt = 0;
        try {
            consumer.commitAsync(Map.copyOf(offsets), (ignored, exception) -> {
                events.add(new CommitDone(submitted, exception));
                LockSupport.unpark(thread);
            });
        } catch (Exception exception) {
            request = null;
            commitRetryAt = failure("COMMIT", exception);
        }
    }

    private void drainEvents() {
        Event event;
        while ((event = events.poll()) != null) {
            if (event instanceof HandlerDone done) {
                TaggedRecord tagged = done.tagged();
                if (busy == tagged) busy = null;
                if (done.fatal() != null) {
                    fatal(done.fatal());
                    continue;
                }
                if (batch != null && batch.id == tagged.batchId() && valid(tagged)) {
                    batch.completed++;
                    dirty.put(tagged.partition(), new Progress(tagged.epoch(),
                            new OffsetAndMetadata(tagged.record().offset() + 1,
                                    tagged.record().leaderEpoch(), "")));
                }
            } else if (event instanceof CommitDone done) {
                if (request != done.request() || request.instance() != instanceId) continue;
                CommitRequest completed = request;
                request = null;
                boolean anyValid = completed.progress().entrySet().stream().anyMatch(entry ->
                        valid(entry.getKey(), completed.instance(), entry.getValue().epoch()));
                if (!anyValid) continue;
                if (done.exception() != null) {
                    commitRetryAt = failure("COMMIT", done.exception());
                } else {
                    completed.progress().forEach((partition, committed) -> {
                        Progress current = dirty.get(partition);
                        if (current != null && current.epoch() == committed.epoch()
                                && valid(partition, completed.instance(), committed.epoch())
                                && current.offset().offset() <= committed.offset().offset()) {
                            dirty.remove(partition);
                        }
                    });
                    commitRetryAt = 0;
                    if (dirty.isEmpty() && pollRetryAt == 0) healthy();
                }
            }
        }
    }

    private void applyFetchState() {
        Set<TopicPartition> assigned = consumer.assignment();
        if (closing || !control.allowed() || batch != null || waitingBatch != null || request != null
                || !dirty.isEmpty() || pollRetryAt != 0 || commitRetryAt != 0) {
            consumer.pause(assigned);
        } else {
            consumer.resume(assigned);
        }
    }

    private ConsumerRebalanceListener rebalanceListener(long subscribedInstance) {
        return new ConsumerRebalanceListener() {
            @Override public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                if (consumer == null || instanceId != subscribedInstance) return;
                drainEvents();
                if (request == null && !dirty.isEmpty()) {
                    // Best effort only: no sync fallback and no waiting in the rebalance callback.
                    submitCommit();
                }
                invalidate(partitions);
            }

            @Override public void onPartitionsLost(Collection<TopicPartition> partitions) {
                if (consumer == null || instanceId != subscribedInstance) return;
                invalidate(partitions);
            }

            @Override public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                if (consumer == null || instanceId != subscribedInstance) return;
                for (TopicPartition partition : partitions) {
                    ownership.computeIfAbsent(partition, ignored -> ++ownershipSequence);
                }
                applyFetchState();
            }
        };
    }

    private void invalidate(Collection<TopicPartition> partitions) {
        for (TopicPartition partition : partitions) {
            ownership.remove(partition);
            dirty.remove(partition);
        }
        if (dirty.isEmpty()) commitRetryAt = 0;
    }

    private boolean valid(TaggedRecord tagged) {
        return valid(tagged.partition(), tagged.instance(), tagged.epoch());
    }

    private boolean valid(TopicPartition partition, long instance, long epoch) {
        return consumer != null && instanceId == instance && Long.valueOf(epoch).equals(ownership.get(partition));
    }

    private long failure(String phase, Exception exception) {
        if (failures < Integer.MAX_VALUE) failures++;
        Duration delay = retry.delay(failures);
        lastPhase = phase;
        lastError = exception.getClass().getName() + ": " + exception.getMessage();
        LOG.error("Listener {} group={} {} failed, attempt={}, retry in {} ms",
                definition.name(), definition.groupId(), phase, failures, delay.toMillis(), exception);
        return System.nanoTime() + delay.toNanos();
    }

    private void healthy() {
        if (createRetryAt != 0 || pollRetryAt != 0 || commitRetryAt != 0 || request != null) return;
        if (failures > 0) LOG.info("Listener {} recovered after {} failures", definition.name(), failures);
        failures = 0;
    }

    private void fatal(Error error) {
        failed = true;
        lastPhase = "FATAL";
        lastError = error.getClass().getName() + ": " + error.getMessage();
        LOG.error("Listener {} stopped after fatal error", definition.name(), error);
    }

    @SuppressWarnings("deprecation") // Consumer.close(Duration) also supports older Kafka client APIs.
    private void retire() {
        Consumer<byte[], byte[]> retired = consumer;
        consumer = null;
        subscribed = false;
        ownership.clear();
        dirty.clear();
        batch = null;
        waitingBatch = null;
        request = null;
        pollRetryAt = 0;
        commitRetryAt = 0;
        if (retired != null) {
            try {
                long nanos = closing ? Math.max(0, closeDeadline - System.nanoTime())
                        : Math.min(options.closeTimeout().toNanos(), TimeUnit.SECONDS.toNanos(5));
                retired.close(Duration.ofNanos(nanos));
            } catch (Exception exception) {
                LOG.warn("Listener {} CLOSE failed", definition.name(), exception);
            }
        }
    }

    private boolean drained() {
        return batch == null && busy == null && request == null && dirty.isEmpty();
    }

    private Duration pollDuration() {
        if (!closing) return options.pollTimeout();
        return Duration.ofNanos(Math.max(0, Math.min(options.pollTimeout().toNanos(),
                closeDeadline - System.nanoTime())));
    }

    private void publish(State terminal) {
        synchronized (manager.controlLock) {
            boolean allowed = control.allowed();
            State state = terminal;
            long retryAt = earliest(createRetryAt, pollRetryAt, commitRetryAt);
            if (state == null) {
                if (failed) state = State.FAILED;
                else if (closing) state = State.CLOSING;
                else if (!control.started) state = State.NEW;
                else if (!allowed) state = batch != null || request != null || !dirty.isEmpty()
                        ? State.PAUSING : State.PAUSED;
                else if (retryAt != 0) state = State.RETRYING;
                else if (consumer == null || !subscribed) state = State.STARTING;
                else state = State.RUNNING;
            }
            CommitState commitState = request != null ? CommitState.IN_FLIGHT
                    : commitRetryAt != 0 ? CommitState.RETRY_WAIT
                    : !dirty.isEmpty() ? CommitState.PENDING : CommitState.NONE;
            Instant nextRetry = retryAt == 0 ? null : Instant.now().plusNanos(Math.max(0, retryAt - System.nanoTime()));
            Batch visible = batch != null ? batch : waitingBatch;
            view = new ListenerSnapshot(definition.name(), state, control.started,
                    control.owners, manager.globalOwners, ownership.keySet(), visible == null ? 0 : visible.id,
                    visible == null ? 0 : visible.records.size(), visible == null ? 0 : visible.completed,
                    commitState, pollCount, lastPhase, lastError, failures, nextRetry);
        }
    }

    private static long earliest(long... values) {
        long result = 0;
        for (long value : values) if (value != 0 && (result == 0 || value - result < 0)) result = value;
        return result;
    }

    private sealed interface Event permits HandlerDone, CommitDone {}
    private record HandlerDone(TaggedRecord tagged, Error fatal) implements Event {}
    private record CommitDone(CommitRequest request, Exception exception) implements Event {}
    private record TaggedRecord(long instance, long epoch, long batchId, TopicPartition partition,
                                ConsumerRecord<byte[], byte[]> record) {}
    private record Progress(long epoch, OffsetAndMetadata offset) {}
    private record CommitRequest(long instance, long sequence, Map<TopicPartition, Progress> progress,
                                 long submittedAt) {}

    private static final class Batch {
        final long id;
        final List<TaggedRecord> records;
        int index;
        int completed;
        Batch(long id, List<TaggedRecord> records) { this.id = id; this.records = List.copyOf(records); }
    }

    private static final class CommitCallbackTimeout extends TimeoutException {
        private static final long serialVersionUID = 1L;
        CommitCallbackTimeout() { super("No async commit callback before watchdog deadline; retire instance"); }
    }
}
