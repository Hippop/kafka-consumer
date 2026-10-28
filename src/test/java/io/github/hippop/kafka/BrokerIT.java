package io.github.hippop.kafka;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import static io.github.hippop.kafka.ListenerManagerTest.await;
import static org.junit.jupiter.api.Assertions.*;

/** Starts a real, separate KRaft JVM. No Docker or pre-existing broker is required. */
@EnabledIfSystemProperty(named = "broker.it.enabled", matches = "true")
class BrokerIT {
    @Test @Timeout(120)
    void subscribeBatchPauseAsyncCommitAndMaintenancePollAgainstRealBroker() throws Exception {
        try (Broker broker = Broker.start(); Admin admin = Admin.create(Map.of(
                "bootstrap.servers", broker.address, "default.api.timeout.ms", 3000,
                "request.timeout.ms", 2000))) {
            long readyDeadline = System.nanoTime() + Duration.ofSeconds(40).toNanos();
            while (true) {
                try { admin.listTopics().names().get(3, TimeUnit.SECONDS); break; }
                catch (Exception unavailable) {
                    if (System.nanoTime() - readyDeadline > 0) {
                        throw new AssertionError("Broker did not start; see " + broker.log, unavailable);
                    }
                    Thread.sleep(100);
                }
            }
            String topic = "batch-events";
            String group = "batch-group";
            admin.createTopics(List.of(new NewTopic(topic, 1, (short) 1))).all().get(10, TimeUnit.SECONDS);
            Properties producerConfig = new Properties();
            producerConfig.put("bootstrap.servers", broker.address);
            producerConfig.put("key.serializer", ByteArraySerializer.class.getName());
            producerConfig.put("value.serializer", ByteArraySerializer.class.getName());
            producerConfig.put("delivery.timeout.ms", 10000);
            producerConfig.put("request.timeout.ms", 3000);
            try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(producerConfig)) {
                for (int i = 0; i < 10; i++) {
                    producer.send(new ProducerRecord<>(topic, new byte[]{(byte) i})).get(10, TimeUnit.SECONDS);
                }
                AtomicInteger builds = new AtomicInteger();
                AtomicInteger handled = new AtomicInteger();
                CountDownLatch atThird = new CountDownLatch(1);
                CountDownLatch release = new CountDownLatch(1);
                ListenerOptions options = new ListenerOptions(Duration.ofMillis(20), Duration.ofMillis(50),
                        Duration.ofMillis(200), Duration.ofSeconds(10), Duration.ofSeconds(5));
                Map<String, Object> config = Map.of("bootstrap.servers", broker.address,
                        "auto.offset.reset", "earliest", "max.poll.records", 10,
                        "max.poll.interval.ms", 1000, "group.protocol", "classic");
                try (ListenerManager manager = new ListenerManager(config, options, properties -> {
                    builds.incrementAndGet();
                    return new KafkaConsumer<>(properties);
                })) {
                    manager.register("orders", List.of(topic), group, record -> {
                        handled.incrementAndGet();
                        if (record.offset() == 2) {
                            manager.pause("orders", "migration");
                            manager.pause("orders", "reload");
                            atThird.countDown();
                            if (!release.await(10, TimeUnit.SECONDS)) throw new IllegalStateException("test release timed out");
                        }
                        if (record.offset() == 4) throw new IllegalArgumentException("expected business failure");
                    });
                    assertEquals(0, builds.get());
                    manager.startAll();
                    try {
                        assertTrue(atThird.await(15, TimeUnit.SECONDS));
                        await(() -> manager.status("orders").batchSize() == 10);
                        long polls = manager.status("orders").pollCount();
                        // Longer than max.poll.interval.ms while the callback is still blocked.
                        Thread.sleep(2200);
                        assertEquals(3, handled.get());
                        assertTrue(manager.status("orders").pollCount() > polls + 20);
                        assertEquals(1, admin.describeConsumerGroups(List.of(group)).all()
                                .get(5, TimeUnit.SECONDS).get(group).members().size());
                        release.countDown();
                        await(() -> handled.get() == 10 && manager.status("orders").state() == ListenerSnapshot.State.PAUSED);
                        TopicPartition partition = new TopicPartition(topic, 0);
                        assertEquals(10, admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata()
                                .get(5, TimeUnit.SECONDS).get(partition).offset());

                        producer.send(new ProducerRecord<>(topic, new byte[]{10})).get(10, TimeUnit.SECONDS);
                        manager.resume("orders", "migration");
                        long pausedPolls = manager.status("orders").pollCount();
                        await(() -> manager.status("orders").pollCount() > pausedPolls + 10);
                        assertEquals(10, handled.get());
                        manager.resume("orders", "reload");
                        await(() -> handled.get() == 11 && manager.status("orders").batchId() == 0);
                        assertEquals(11, admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata()
                                .get(5, TimeUnit.SECONDS).get(partition).offset());
                        assertEquals(1, builds.get());
                    } finally { release.countDown(); }
                }
            }
        }
    }

    private static final class Broker implements AutoCloseable {
        final Process process;
        final String address;
        final Path log;
        private Broker(Process process, String address, Path log) {
            this.process = process; this.address = address; this.log = log;
        }

        static Broker start() throws Exception {
            Path root = Path.of("target", "broker-it").toAbsolutePath();
            Files.createDirectories(root);
            Path directory = Files.createTempDirectory(root, "kraft-");
            int brokerPort = freePort();
            int controllerPort;
            do { controllerPort = freePort(); } while (controllerPort == brokerPort);
            String address = "127.0.0.1:" + brokerPort;
            Path config = directory.resolve("server.properties");
            Files.writeString(config, """
                    process.roles=broker,controller
                    node.id=1
                    listeners=PLAINTEXT://127.0.0.1:%d,CONTROLLER://127.0.0.1:%d
                    advertised.listeners=PLAINTEXT://127.0.0.1:%d
                    controller.listener.names=CONTROLLER
                    listener.security.protocol.map=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT
                    inter.broker.listener.name=PLAINTEXT
                    controller.quorum.voters=1@127.0.0.1:%d
                    log.dirs=%s
                    offsets.topic.replication.factor=1
                    offsets.topic.num.partitions=1
                    transaction.state.log.replication.factor=1
                    transaction.state.log.min.isr=1
                    group.initial.rebalance.delay.ms=0
                    """.formatted(brokerPort, controllerPort, brokerPort, controllerPort,
                    directory.resolve("data").toString()));
            Path storageLog = directory.resolve("storage.log");
            Process storage = launch(storageLog, "kafka.tools.StorageTool", "format", "--cluster-id",
                    Uuid.randomUuid().toString(), "--config", config.toString());
            if (!storage.waitFor(30, TimeUnit.SECONDS)) {
                storage.destroyForcibly();
                throw new AssertionError("Storage format timed out: " + storageLog);
            }
            if (storage.exitValue() != 0) throw new AssertionError(Files.readString(storageLog));
            Path log = directory.resolve("broker.log");
            return new Broker(launch(log, "kafka.Kafka", config.toString()), address, log);
        }

        private static Process launch(Path log, String main, String... args) throws IOException {
            List<String> command = new java.util.ArrayList<>(List.of(
                    Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                    "-Dorg.slf4j.simpleLogger.defaultLogLevel=warn", "-cp",
                    System.getProperty("surefire.test.class.path", System.getProperty("java.class.path")), main));
            command.addAll(List.of(args));
            return new ProcessBuilder(command).redirectErrorStream(true).redirectOutput(log.toFile()).start();
        }

        private static int freePort() throws IOException {
            try (ServerSocket socket = new ServerSocket(0)) { return socket.getLocalPort(); }
        }

        @Override public void close() throws InterruptedException {
            process.destroy();
            if (!process.waitFor(10, TimeUnit.SECONDS)) {
                process.destroyForcibly();
                process.waitFor(5, TimeUnit.SECONDS);
            }
        }
    }

}
