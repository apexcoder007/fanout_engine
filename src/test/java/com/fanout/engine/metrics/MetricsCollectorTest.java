package com.fanout.engine.metrics;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MetricsCollectorTest {

    @Test
    void shouldTrackCountsAndProduceSnapshot() {
        MetricsCollector collector = new MetricsCollector();
        collector.registerSink("rest");
        collector.markIngestedRecord();
        collector.markIngestedRecord();
        collector.markSuccess("rest");
        collector.markFailure("rest");

        MetricsSnapshot snapshot = collector.snapshot();

        assertEquals(2, snapshot.recordsIngested());
        assertEquals(2, snapshot.accountedDeliveries());
        assertEquals(1, snapshot.sinkViews().get("rest").success());
        assertEquals(1, snapshot.sinkViews().get("rest").failure());
        assertTrue(snapshot.ingestThroughputPerSecond() >= 0);
    }

    @Test
    void shouldFormatHumanReadableStatusLine() {
        MetricsCollector collector = new MetricsCollector();
        collector.registerSink("mq");
        collector.markIngestedRecord();
        collector.markSuccess("mq");

        String status = collector.formatStatusLine();

        assertTrue(status.contains("[status]"));
        assertTrue(status.contains("ingested=1"));
        assertTrue(status.contains("mq(success=1, failure=0)"));
    }
}
