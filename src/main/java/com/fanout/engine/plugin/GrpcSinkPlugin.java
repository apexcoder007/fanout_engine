package com.fanout.engine.plugin;

import com.fanout.engine.config.SinkConfig;
import com.fanout.engine.config.SinkType;
import com.fanout.engine.sink.GrpcSink;
import com.fanout.engine.sink.Sink;
import com.fanout.engine.transform.ProtobufTransformer;
import com.fanout.engine.transform.Transformer;

public final class GrpcSinkPlugin implements SinkPlugin<byte[]> {
    @Override
    public SinkType type() {
        return SinkType.GRPC;
    }

    @Override
    public Transformer<byte[]> createTransformer() {
        return new ProtobufTransformer();
    }

    @Override
    public Sink<byte[]> createSink(SinkConfig config) {
        return new GrpcSink(config);
    }
}
