package io.github.hippop.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;

/** Returning, or throwing an ordinary exception, completes this record. */
@FunctionalInterface
public interface MessageListener {
    void onMessage(ConsumerRecord<byte[], byte[]> record) throws Exception;
}
