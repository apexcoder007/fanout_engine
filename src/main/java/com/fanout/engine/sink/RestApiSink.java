package com.fanout.engine.sink;

import com.fanout.engine.config.SinkConfig;
import com.fanout.engine.config.SinkType;

public final class RestApiSink extends AbstractMockSink<String> {
    public RestApiSink(SinkConfig sinkConfig) {
        super(sinkConfig);
    }

    @Override
    public SinkType type() {
        return SinkType.REST_API;
    }

    @Override
    public void send(String payload) throws Exception {
        simulateNetworkOrIo();
        if (payload == null || payload.isBlank()) {
            throw new IllegalArgumentException("REST payload cannot be empty");
        }
    }
}
