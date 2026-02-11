package com.fanout.engine.metrics;

import java.util.concurrent.atomic.LongAdder;

public final class SinkMetrics {
    private final LongAdder success = new LongAdder();
    private final LongAdder failure = new LongAdder();

    public void markSuccess() {
        success.increment();
    }

    public void markFailure() {
        failure.increment();
    }

    public long successCount() {
        return success.sum();
    }

    public long failureCount() {
        return failure.sum();
    }

    public long total() {
        return success.sum() + failure.sum();
    }
}
