package com.fanout.engine.sink;

import com.fanout.engine.config.SinkConfig;
import com.fanout.engine.config.SinkType;

public final class GrpcSink extends AbstractMockSink<byte[]> {
    public GrpcSink(SinkConfig sinkConfig) {
        super(sinkConfig);
    }

    @Override
    public SinkType type() {
        return SinkType.GRPC;
    }

    @Override
    public void send(byte[] payload) throws Exception {
        simulateNetworkOrIo();
        if (payload == null || payload.length == 0) {
            throw new IllegalArgumentException("gRPC payload cannot be empty");
        }
    }
}
