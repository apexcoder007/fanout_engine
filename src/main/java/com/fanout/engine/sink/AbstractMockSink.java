package com.fanout.engine.sink;

import com.fanout.engine.config.SinkConfig;
import com.fanout.engine.config.SinkType;

import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;

public abstract class AbstractMockSink<T> implements Sink<T> {
    protected final SinkConfig sinkConfig;

    protected AbstractMockSink(SinkConfig sinkConfig) {
        this.sinkConfig = sinkConfig;
    }

    @Override
    public String name() {
        return sinkConfig.getName();
    }

    @Override
    public abstract SinkType type();

    protected void simulateNetworkOrIo() throws InterruptedException, IOException {
        int min = Math.max(0, sinkConfig.getMinLatencyMs());
        int max = Math.max(min, sinkConfig.getMaxLatencyMs());
        int latencyMs = ThreadLocalRandom.current().nextInt(min, max + 1);
        if (latencyMs > 0) {
            Thread.sleep(latencyMs);
        }

        double failureRate = sinkConfig.getFailureRate();
        if (failureRate > 0 && ThreadLocalRandom.current().nextDouble() < failureRate) {
            throw new IOException("Simulated downstream failure for sink " + sinkConfig.getName());
        }
    }
}
