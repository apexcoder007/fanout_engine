package com.fanout.engine.plugin;

import com.fanout.engine.config.SinkConfig;
import com.fanout.engine.config.SinkType;
import com.fanout.engine.sink.Sink;
import com.fanout.engine.transform.Transformer;

public interface SinkPlugin<T> {
    SinkType type();

    Transformer<T> createTransformer();

    Sink<T> createSink(SinkConfig config);
}
