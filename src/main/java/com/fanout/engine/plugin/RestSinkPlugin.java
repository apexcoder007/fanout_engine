package com.fanout.engine.plugin;

import com.fanout.engine.config.SinkConfig;
import com.fanout.engine.config.SinkType;
import com.fanout.engine.sink.RestApiSink;
import com.fanout.engine.sink.Sink;
import com.fanout.engine.transform.JsonTransformer;
import com.fanout.engine.transform.Transformer;

public final class RestSinkPlugin implements SinkPlugin<String> {
    @Override
    public SinkType type() {
        return SinkType.REST_API;
    }

    @Override
    public Transformer<String> createTransformer() {
        return new JsonTransformer();
    }

    @Override
    public Sink<String> createSink(SinkConfig config) {
        return new RestApiSink(config);
    }
}
