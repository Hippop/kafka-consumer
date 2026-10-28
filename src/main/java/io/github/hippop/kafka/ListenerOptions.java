package io.github.hippop.kafka;

import java.time.Duration;
import java.util.Objects;

/** Framework timers. Kafka request timeouts are configured separately. */
public record ListenerOptions(Duration pollTimeout, Duration initialRetry,
                              Duration maxRetry, Duration commitCallbackTimeout,
                              Duration closeTimeout) {
    public ListenerOptions {
        positive(pollTimeout, "pollTimeout");
        positive(initialRetry, "initialRetry");
        positive(maxRetry, "maxRetry");
        positive(commitCallbackTimeout, "commitCallbackTimeout");
        positive(closeTimeout, "closeTimeout");
        if (maxRetry.compareTo(initialRetry) < 0) {
            throw new IllegalArgumentException("maxRetry must be >= initialRetry");
        }
    }

    public static ListenerOptions defaults() {
        return new ListenerOptions(Duration.ofMillis(100), Duration.ofSeconds(30),
                Duration.ofMinutes(30), Duration.ofMinutes(2), Duration.ofSeconds(30));
    }

    private static void positive(Duration value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isZero() || value.isNegative()) {
            throw new IllegalArgumentException(name + " must be positive");
        }
        // Reject values that cannot be represented by the monotonic timers.
        value.toNanos();
    }
}
