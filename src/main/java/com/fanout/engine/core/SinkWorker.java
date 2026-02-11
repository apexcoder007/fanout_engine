package com.fanout.engine.core;

import com.fanout.engine.metrics.MetricsCollector;
import com.fanout.engine.model.RecordEnvelope;
import com.fanout.engine.model.SourceRecord;
import com.fanout.engine.resilience.DeadLetterQueue;

import java.util.concurrent.BlockingQueue;

public final class SinkWorker<T> implements Runnable {
    private final SinkPipeline<T> pipeline;
    private final MetricsCollector metrics;
    private final DeadLetterQueue deadLetterQueue;
    private final int maxRetries;
    private final long retryBackoffMillis;

    public SinkWorker(SinkPipeline<T> pipeline,
                      MetricsCollector metrics,
                      DeadLetterQueue deadLetterQueue,
                      int maxRetries,
                      long retryBackoffMillis) {
        this.pipeline = pipeline;
        this.metrics = metrics;
        this.deadLetterQueue = deadLetterQueue;
        this.maxRetries = maxRetries;
        this.retryBackoffMillis = retryBackoffMillis;
    }

    @Override
    public void run() {
        BlockingQueue<RecordEnvelope> queue = pipeline.queue();
        while (!Thread.currentThread().isInterrupted()) {
            try {
                RecordEnvelope envelope = queue.take();
                if (envelope.isPoison()) {
                    return;
                }
                process(envelope.record());
            } catch (InterruptedException interruptedException) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    private void process(SourceRecord record) {
        T payload;
        try {
            payload = pipeline.transformer().transform(record);
        } catch (Exception transformException) {
            metrics.markFailure(pipeline.config().getName());
            deadLetterQueue.writeFailure(record, pipeline.config().getName(), 1, transformException);
            return;
        }

        int totalAttempts = Math.max(1, maxRetries + 1);
        Exception lastError = null;

        for (int attempt = 1; attempt <= totalAttempts; attempt++) {
            try {
                pipeline.rateLimiter().acquire();
                pipeline.sink().send(payload);
                metrics.markSuccess(pipeline.config().getName());
                return;
            } catch (Exception exception) {
                lastError = exception;
                if (attempt < totalAttempts) {
                    sleepWithBackoff(attempt);
                }
            }
        }

        metrics.markFailure(pipeline.config().getName());
        deadLetterQueue.writeFailure(record, pipeline.config().getName(), totalAttempts, lastError);
    }

    private void sleepWithBackoff(int attempt) {
        long boundedShift = Math.min(20, Math.max(0, attempt - 1));
        long multiplier = 1L << boundedShift;
        long waitMillis = retryBackoffMillis * multiplier;
        if (waitMillis <= 0) {
            return;
        }

        try {
            Thread.sleep(waitMillis);
        } catch (InterruptedException interruptedException) {
            Thread.currentThread().interrupt();
        }
    }
}
