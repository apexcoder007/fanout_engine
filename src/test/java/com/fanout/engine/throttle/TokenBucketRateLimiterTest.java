package com.fanout.engine.throttle;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TokenBucketRateLimiterTest {

    @Test
    void shouldRejectNonPositivePermitRates() {
        assertThrows(IllegalArgumentException.class, () -> new TokenBucketRateLimiter(0));
        assertThrows(IllegalArgumentException.class, () -> new TokenBucketRateLimiter(-5));
    }

    @Test
    void shouldThrottleWhenTokensExhausted() {
        TokenBucketRateLimiter limiter = new TokenBucketRateLimiter(1.0);

        limiter.acquire();
        long startNanos = System.nanoTime();
        limiter.acquire();
        long elapsedMillis = (System.nanoTime() - startNanos) / 1_000_000;

        assertTrue(elapsedMillis >= 800, "Expected second acquire to block near 1 second, actual=" + elapsedMillis + "ms");
    }

    @Test
    void shouldAllowBurstAtHighRate() {
        TokenBucketRateLimiter limiter = new TokenBucketRateLimiter(100_000.0);
        long startNanos = System.nanoTime();
        for (int i = 0; i < 100; i++) {
            limiter.acquire();
        }
        long elapsedMillis = (System.nanoTime() - startNanos) / 1_000_000;

        assertTrue(elapsedMillis < 250, "Expected fast acquires at high rate, actual=" + elapsedMillis + "ms");
    }
}
