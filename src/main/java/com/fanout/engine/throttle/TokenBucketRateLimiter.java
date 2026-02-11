package com.fanout.engine.throttle;

import java.util.concurrent.locks.LockSupport;

public final class TokenBucketRateLimiter implements RateLimiter {
    private final double tokensPerNano;
    private final double maxTokens;

    private double availableTokens;
    private long lastRefillNanos;

    public TokenBucketRateLimiter(double permitsPerSecond) {
        if (permitsPerSecond <= 0) {
            throw new IllegalArgumentException("permitsPerSecond must be > 0");
        }
        this.tokensPerNano = permitsPerSecond / 1_000_000_000.0;
        this.maxTokens = Math.max(1.0, permitsPerSecond);
        this.availableTokens = this.maxTokens;
        this.lastRefillNanos = System.nanoTime();
    }

    @Override
    public void acquire() {
        while (true) {
            long waitNanos = tryAcquire();
            if (waitNanos <= 0) {
                return;
            }
            LockSupport.parkNanos(waitNanos);
        }
    }

    private synchronized long tryAcquire() {
        refill();
        if (availableTokens >= 1.0) {
            availableTokens -= 1.0;
            return 0;
        }

        double missingTokens = 1.0 - availableTokens;
        return (long) Math.ceil(missingTokens / tokensPerNano);
    }

    private void refill() {
        long now = System.nanoTime();
        long elapsed = now - lastRefillNanos;
        if (elapsed <= 0) {
            return;
        }

        availableTokens = Math.min(maxTokens, availableTokens + elapsed * tokensPerNano);
        lastRefillNanos = now;
    }
}
