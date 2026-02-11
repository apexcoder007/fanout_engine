package com.fanout.engine.metrics;

import java.time.Duration;
import java.util.Map;

public record MetricsSnapshot(
        Duration elapsed,
        long recordsIngested,
        double ingestThroughputPerSecond,
        long accountedDeliveries,
        Map<String, SinkMetricsView> sinkViews
) {
    public record SinkMetricsView(long success, long failure) {
    }
}
