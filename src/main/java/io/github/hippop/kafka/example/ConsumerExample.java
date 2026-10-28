package io.github.hippop.kafka.example;

import io.github.hippop.kafka.KafkaMessageListener;
import io.github.hippop.kafka.ListenerManager;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Minimal runnable example: args[0] is bootstrap.servers; topic/group are in the annotation. */
public final class ConsumerExample {
    private static final Logger LOG = LoggerFactory.getLogger(ConsumerExample.class);

    private ConsumerExample() {}

    public static void main(String[] args) throws InterruptedException {
        if (args.length != 1) throw new IllegalArgumentException("Usage: ConsumerExample <bootstrap.servers>");
        ListenerManager manager = ListenerManager.create(Map.of(
                "bootstrap.servers", args[0], "auto.offset.reset", "earliest"));
        Runtime.getRuntime().addShutdownHook(new Thread(manager::close, "consumer-shutdown"));
        try (manager) {
            manager.register(new Orders());
            // Application readiness checks can happen before this explicit call.
            manager.start("orders");
            new CountDownLatch(1).await();
        }
    }

    public static final class Orders {
        @KafkaMessageListener(name = "orders", topics = "order-events", groupId = "order-service")
        public void handle(ConsumerRecord<byte[], byte[]> record) {
            LOG.info("Handling topic={} partition={} offset={}", record.topic(), record.partition(), record.offset());
        }
    }
}
