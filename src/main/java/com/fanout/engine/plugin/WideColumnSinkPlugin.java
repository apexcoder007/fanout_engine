package com.fanout.engine.plugin;

import com.fanout.engine.config.SinkConfig;
import com.fanout.engine.config.SinkType;
import com.fanout.engine.model.WideColumnPayload;
import com.fanout.engine.sink.Sink;
import com.fanout.engine.sink.WideColumnDbSink;
import com.fanout.engine.transform.Transformer;
import com.fanout.engine.transform.WideColumnTransformer;

public final class WideColumnSinkPlugin implements SinkPlugin<WideColumnPayload> {
    @Override
    public SinkType type() {
        return SinkType.WIDE_COLUMN_DB;
    }

    @Override
    public Transformer<WideColumnPayload> createTransformer() {
        return new WideColumnTransformer();
    }

    @Override
    public Sink<WideColumnPayload> createSink(SinkConfig config) {
        return new WideColumnDbSink(config);
    }
}
