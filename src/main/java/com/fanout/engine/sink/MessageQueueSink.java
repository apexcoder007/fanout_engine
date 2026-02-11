package com.fanout.engine.sink;

import com.fanout.engine.config.SinkConfig;
import com.fanout.engine.config.SinkType;

public final class MessageQueueSink extends AbstractMockSink<String> {
    public MessageQueueSink(SinkConfig sinkConfig) {
        super(sinkConfig);
    }

    @Override
    public SinkType type() {
        return SinkType.MESSAGE_QUEUE;
    }

    @Override
    public void send(String payload) throws Exception {
        simulateNetworkOrIo();
        if (payload == null || payload.isBlank()) {
            throw new IllegalArgumentException("Message queue payload cannot be empty");
        }
    }
}
