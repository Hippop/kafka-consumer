package io.github.hippop.kafka;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;

/** Test driver: all mock mutations/callbacks execute on the container's IO thread. */
final class ScriptedConsumer extends MockConsumer<byte[], byte[]> {
    static final TopicPartition TP = new TopicPartition("events", 0);
    final ConcurrentLinkedQueue<Runnable> commands = new ConcurrentLinkedQueue<>();
    final CopyOnWriteArrayList<Request> commits = new CopyOnWriteArrayList<>();
    final AtomicInteger polls = new AtomicInteger();
    final AtomicInteger subscriptions = new AtomicInteger();
    final AtomicInteger commitInvocations = new AtomicInteger();
    final AtomicBoolean automaticAck = new AtomicBoolean(true);
    final AtomicBoolean closedFlag = new AtomicBoolean();
    final AtomicBoolean wakeupRequested = new AtomicBoolean();
    final Set<String> operationThreads = java.util.concurrent.ConcurrentHashMap.newKeySet();
    private final Deque<Request> awaiting = new ArrayDeque<>();
    private ConsumerRebalanceListener listener;
    volatile Set<TopicPartition> lastPaused = Set.of();
    volatile RuntimeException subscribeFailure;
    volatile RuntimeException commitFailure;
    volatile Runnable beforeFirstRecords;

    ScriptedConsumer() { super("earliest"); }

    void initialRecords(int count) {
        commands.add(() -> {
            reassign(List.of(TP));
            for (int i = 0; i < count; i++) addRecord(record(i));
        });
    }

    void addLater(long offset) { commands.add(() -> addRecord(record(offset))); }

    void reassign(Collection<TopicPartition> partitions) {
        Map<TopicPartition, Long> beginning = new HashMap<>();
        for (TopicPartition partition : partitions) beginning.put(partition, 0L);
        updateBeginningOffsets(beginning);
        super.rebalance(partitions);
    }

    void loseAll() {
        listener.onPartitionsLost(Set.copyOf(assignment()));
        super.rebalance(List.of());
    }

    void acknowledge(Exception exception) {
        commands.add(() -> {
            Request request = awaiting.removeFirst();
            request.callback().onComplete(request.offsets(), exception);
        });
    }

    @Override public synchronized void subscribe(Collection<String> topics, ConsumerRebalanceListener callback) {
        operation();
        subscriptions.incrementAndGet();
        if (subscribeFailure != null) throw subscribeFailure;
        listener = callback;
        super.subscribe(topics, callback);
    }

    @Override public synchronized ConsumerRecords<byte[], byte[]> poll(Duration timeout) {
        operation();
        polls.incrementAndGet();
        if (wakeupRequested.getAndSet(false)) throw new WakeupException();
        Runnable command;
        while ((command = commands.poll()) != null) command.run();
        if (automaticAck.get()) {
            while (!awaiting.isEmpty()) {
                Request request = awaiting.removeFirst();
                request.callback().onComplete(request.offsets(), null);
            }
        }
        ConsumerRecords<byte[], byte[]> records = super.poll(Duration.ZERO);
        if (!records.isEmpty() && beforeFirstRecords != null) {
            Runnable action = beforeFirstRecords;
            beforeFirstRecords = null;
            action.run();
        }
        return records;
    }

    @Override public synchronized void commitAsync(Map<TopicPartition, OffsetAndMetadata> offsets,
                                                   OffsetCommitCallback callback) {
        operation();
        commitInvocations.incrementAndGet();
        if (commitFailure != null) {
            RuntimeException failure = commitFailure;
            commitFailure = null;
            throw failure;
        }
        Request request = new Request(Map.copyOf(offsets), callback);
        commits.add(request);
        awaiting.addLast(request);
    }

    @Override public synchronized void pause(Collection<TopicPartition> partitions) {
        operation();
        super.pause(partitions);
        lastPaused = Set.copyOf(paused());
    }

    @Override public synchronized void resume(Collection<TopicPartition> partitions) {
        operation();
        super.resume(partitions);
        lastPaused = Set.copyOf(paused());
    }

    @Override public void wakeup() { wakeupRequested.set(true); }

    @SuppressWarnings("deprecation")
    @Override public synchronized void close(Duration timeout) {
        operation();
        closedFlag.set(true);
        super.close(timeout);
    }

    private void operation() {
        String name = Thread.currentThread().getName();
        operationThreads.add(name);
        if (!name.endsWith("-consumer")) throw new AssertionError("Consumer accessed outside IO thread: " + name);
    }

    static ConsumerRecord<byte[], byte[]> record(long offset) {
        return new ConsumerRecord<>(TP.topic(), TP.partition(), offset, null, new byte[]{(byte) offset});
    }

    record Request(Map<TopicPartition, OffsetAndMetadata> offsets, OffsetCommitCallback callback) {}
}
