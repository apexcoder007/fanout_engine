package com.fanout.engine.plugin;

import com.fanout.engine.config.SinkConfig;
import com.fanout.engine.config.SinkType;
import com.fanout.engine.sink.MessageQueueSink;
import com.fanout.engine.sink.Sink;
import com.fanout.engine.transform.Transformer;
import com.fanout.engine.transform.XmlTransformer;

public final class MessageQueueSinkPlugin implements SinkPlugin<String> {
    @Override
    public SinkType type() {
        return SinkType.MESSAGE_QUEUE;
    }

    @Override
    public Transformer<String> createTransformer() {
        return new XmlTransformer();
    }

    @Override
    public Sink<String> createSink(SinkConfig config) {
        return new MessageQueueSink(config);
    }
}
