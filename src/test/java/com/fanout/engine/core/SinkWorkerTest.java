package com.fanout.engine.core;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fanout.engine.config.SinkConfig;
import com.fanout.engine.config.SinkType;
import com.fanout.engine.metrics.MetricsCollector;
import com.fanout.engine.model.RecordEnvelope;
import com.fanout.engine.model.SourceRecord;
import com.fanout.engine.resilience.DeadLetterQueue;
import com.fanout.engine.sink.Sink;
import com.fanout.engine.throttle.RateLimiter;
import com.fanout.engine.transform.Transformer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SinkWorkerTest {

    @TempDir
    Path tempDir;

    @Test
    void shouldMarkSuccessOnFirstAttempt() throws Exception {
        BlockingQueue<RecordEnvelope> queue = new ArrayBlockingQueue<>(4);
        queue.put(RecordEnvelope.fromRecord(record(1)));
        queue.put(RecordEnvelope.poison());

        CountingRateLimiter limiter = new CountingRateLimiter();
        AtomicInteger attempts = new AtomicInteger();

        SinkPipeline<String> pipeline = pipeline(
                queue,
                source -> "payload-" + source.sequence(),
                sink(payload -> attempts.incrementAndGet()),
                limiter
        );

        MetricsCollector metrics = new MetricsCollector();
        metrics.registerSink("test");
        Path dlqPath = tempDir.resolve("dlq.jsonl");

        try (DeadLetterQueue deadLetterQueue = new DeadLetterQueue(dlqPath)) {
            new SinkWorker<>(pipeline, metrics, deadLetterQueue, 3, 1).run();
        }

        assertEquals(1, attempts.get());
        assertEquals(1, limiter.acquireCount.get());
        assertEquals(1, metrics.snapshot().sinkViews().get("test").success());
        assertEquals(0, metrics.snapshot().sinkViews().get("test").failure());
        assertTrue(Files.readString(dlqPath).isBlank());
    }

    @Test
    void shouldRetryAndEventuallySucceed() throws Exception {
        BlockingQueue<RecordEnvelope> queue = new ArrayBlockingQueue<>(4);
        queue.put(RecordEnvelope.fromRecord(record(1)));
        queue.put(RecordEnvelope.poison());

        CountingRateLimiter limiter = new CountingRateLimiter();
        AtomicInteger attempts = new AtomicInteger();

        SinkPipeline<String> pipeline = pipeline(
                queue,
                source -> "payload-" + source.sequence(),
                sink(payload -> {
                    int current = attempts.incrementAndGet();
                    if (current < 3) {
                        throw new RuntimeException("temporary");
                    }
                }),
                limiter
        );

        MetricsCollector metrics = new MetricsCollector();
        metrics.registerSink("test");
        Path dlqPath = tempDir.resolve("dlq.jsonl");

        try (DeadLetterQueue deadLetterQueue = new DeadLetterQueue(dlqPath)) {
            new SinkWorker<>(pipeline, metrics, deadLetterQueue, 3, 1).run();
        }

        assertEquals(3, attempts.get());
        assertEquals(3, limiter.acquireCount.get());
        assertEquals(1, metrics.snapshot().sinkViews().get("test").success());
        assertEquals(0, metrics.snapshot().sinkViews().get("test").failure());
        assertTrue(Files.readString(dlqPath).isBlank());
    }

    @Test
    void shouldWriteDlqWhenRetriesExhausted() throws Exception {
        BlockingQueue<RecordEnvelope> queue = new ArrayBlockingQueue<>(4);
        queue.put(RecordEnvelope.fromRecord(record(3)));
        queue.put(RecordEnvelope.poison());

        CountingRateLimiter limiter = new CountingRateLimiter();
        AtomicInteger attempts = new AtomicInteger();

        SinkPipeline<String> pipeline = pipeline(
                queue,
                source -> "payload-" + source.sequence(),
                sink(payload -> {
                    attempts.incrementAndGet();
                    throw new RuntimeException("down");
                }),
                limiter
        );

        MetricsCollector metrics = new MetricsCollector();
        metrics.registerSink("test");
        Path dlqPath = tempDir.resolve("dlq.jsonl");

        try (DeadLetterQueue deadLetterQueue = new DeadLetterQueue(dlqPath)) {
            new SinkWorker<>(pipeline, metrics, deadLetterQueue, 2, 1).run();
        }

        assertEquals(3, attempts.get());
        assertEquals(3, limiter.acquireCount.get());
        assertEquals(0, metrics.snapshot().sinkViews().get("test").success());
        assertEquals(1, metrics.snapshot().sinkViews().get("test").failure());

        JsonNode node = new ObjectMapper().readTree(Files.readAllLines(dlqPath).getFirst());
        assertEquals(3, node.get("recordSequence").asInt());
        assertEquals(3, node.get("attempts").asInt());
        assertEquals("test", node.get("sinkName").asText());
    }

    @Test
    void shouldWriteDlqWhenTransformFailsWithoutCallingSink() throws Exception {
        BlockingQueue<RecordEnvelope> queue = new ArrayBlockingQueue<>(4);
        queue.put(RecordEnvelope.fromRecord(record(5)));
        queue.put(RecordEnvelope.poison());

        CountingRateLimiter limiter = new CountingRateLimiter();
        AtomicInteger sinkAttempts = new AtomicInteger();

        SinkPipeline<String> pipeline = pipeline(
                queue,
                source -> {
                    throw new RuntimeException("bad input");
                },
                sink(payload -> sinkAttempts.incrementAndGet()),
                limiter
        );

        MetricsCollector metrics = new MetricsCollector();
        metrics.registerSink("test");
        Path dlqPath = tempDir.resolve("dlq.jsonl");

        try (DeadLetterQueue deadLetterQueue = new DeadLetterQueue(dlqPath)) {
            new SinkWorker<>(pipeline, metrics, deadLetterQueue, 3, 1).run();
        }

        assertEquals(0, sinkAttempts.get());
        assertEquals(0, limiter.acquireCount.get());
        assertEquals(0, metrics.snapshot().sinkViews().get("test").success());
        assertEquals(1, metrics.snapshot().sinkViews().get("test").failure());

        JsonNode node = new ObjectMapper().readTree(Files.readAllLines(dlqPath).getFirst());
        assertEquals(1, node.get("attempts").asInt());
        assertEquals("bad input", node.get("error").asText());
    }

    private SinkPipeline<String> pipeline(BlockingQueue<RecordEnvelope> queue,
                                          Transformer<String> transformer,
                                          Sink<String> sink,
                                          RateLimiter rateLimiter) {
        SinkConfig sinkConfig = new SinkConfig();
        sinkConfig.setName("test");
        sinkConfig.setType(SinkType.REST_API);
        sinkConfig.setWorkers(1);

        return new SinkPipeline<>(sinkConfig, transformer, sink, queue, rateLimiter, 1);
    }

    private Sink<String> sink(ThrowingStringConsumer sendConsumer) {
        return new Sink<>() {
            @Override
            public String name() {
                return "test-sink";
            }

            @Override
            public SinkType type() {
                return SinkType.REST_API;
            }

            @Override
            public void send(String payload) throws Exception {
                sendConsumer.accept(payload);
            }
        };
    }

    private SourceRecord record(long sequence) {
        Map<String, Object> fields = new LinkedHashMap<>();
        fields.put("id", sequence);
        return new SourceRecord(sequence, fields, "raw");
    }

    private static final class CountingRateLimiter implements RateLimiter {
        private final AtomicInteger acquireCount = new AtomicInteger();

        @Override
        public void acquire() {
            acquireCount.incrementAndGet();
        }
    }

    @FunctionalInterface
    private interface ThrowingStringConsumer {
        void accept(String value) throws Exception;
    }
}
