package com.fanout.engine.metrics;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

public final class MetricsCollector {
    private final long startedAtNanos = System.nanoTime();
    private final LongAdder recordsIngested = new LongAdder();
    private final Map<String, SinkMetrics> sinkMetrics = new ConcurrentHashMap<>();

    public void registerSink(String sinkName) {
        sinkMetrics.computeIfAbsent(sinkName, ignored -> new SinkMetrics());
    }

    public void markIngestedRecord() {
        recordsIngested.increment();
    }

    public void markSuccess(String sinkName) {
        sinkMetrics.computeIfAbsent(sinkName, ignored -> new SinkMetrics()).markSuccess();
    }

    public void markFailure(String sinkName) {
        sinkMetrics.computeIfAbsent(sinkName, ignored -> new SinkMetrics()).markFailure();
    }

    public long ingestedCount() {
        return recordsIngested.sum();
    }

    public long totalAccountedDeliveries() {
        return sinkMetrics.values().stream().mapToLong(SinkMetrics::total).sum();
    }

    public MetricsSnapshot snapshot() {
        long now = System.nanoTime();
        long elapsedNanos = Math.max(1L, now - startedAtNanos);
        Duration elapsed = Duration.ofNanos(elapsedNanos);
        long ingested = recordsIngested.sum();
        double throughput = ingested / (elapsedNanos / 1_000_000_000.0);

        Map<String, MetricsSnapshot.SinkMetricsView> sinkViews = new LinkedHashMap<>();
        for (Map.Entry<String, SinkMetrics> entry : sinkMetrics.entrySet()) {
            sinkViews.put(entry.getKey(),
                    new MetricsSnapshot.SinkMetricsView(entry.getValue().successCount(), entry.getValue().failureCount()));
        }

        return new MetricsSnapshot(elapsed, ingested, throughput, totalAccountedDeliveries(), sinkViews);
    }

    public String formatStatusLine() {
        MetricsSnapshot snapshot = snapshot();
        StringBuilder sb = new StringBuilder();
        sb.append("[status] elapsed=").append(snapshot.elapsed().toSeconds()).append("s")
                .append(" ingested=").append(snapshot.recordsIngested())
                .append(" throughput=").append(String.format("%.2f", snapshot.ingestThroughputPerSecond())).append(" rec/s")
                .append(" accountedDeliveries=").append(snapshot.accountedDeliveries());

        for (Map.Entry<String, MetricsSnapshot.SinkMetricsView> entry : snapshot.sinkViews().entrySet()) {
            sb.append(" | ").append(entry.getKey())
                    .append("(success=").append(entry.getValue().success())
                    .append(", failure=").append(entry.getValue().failure())
                    .append(")");
        }
        return sb.toString();
    }
}
