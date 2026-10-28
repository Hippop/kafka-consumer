package io.github.hippop.kafka;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/** Declares a synchronous, single-record listener on an explicitly registered object. */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface KafkaMessageListener {
    String name();
    String[] topics();
    String groupId();
}
