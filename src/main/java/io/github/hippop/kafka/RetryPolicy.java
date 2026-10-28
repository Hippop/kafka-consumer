package io.github.hippop.kafka;

import java.time.Duration;

/** Overflow-safe capped exponential backoff. Failure number 1 uses the initial delay. */
public final class RetryPolicy {
    private final long initialNanos;
    private final long maximumNanos;

    public RetryPolicy(Duration initial, Duration maximum) {
        if (initial.isNegative() || initial.isZero() || maximum.compareTo(initial) < 0) {
            throw new IllegalArgumentException("Require 0 < initial <= maximum");
        }
        initialNanos = initial.toNanos();
        maximumNanos = maximum.toNanos();
    }

    public Duration delay(int failureNumber) {
        if (failureNumber < 1) throw new IllegalArgumentException("failureNumber must be >= 1");
        long delay = initialNanos;
        for (int i = 1; i < failureNumber && delay < maximumNanos; i++) {
            delay = delay > maximumNanos / 2 ? maximumNanos : Math.min(maximumNanos, delay * 2);
        }
        return Duration.ofNanos(delay);
    }
}
