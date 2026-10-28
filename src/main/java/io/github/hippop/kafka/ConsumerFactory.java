package io.github.hippop.kafka;

import java.util.Map;
import org.apache.kafka.clients.consumer.Consumer;

/** Called on the consumer thread; each invocation must return a fresh consumer. */
@FunctionalInterface
public interface ConsumerFactory {
    Consumer<byte[], byte[]> create(Map<String, Object> configuration) throws Exception;
}
