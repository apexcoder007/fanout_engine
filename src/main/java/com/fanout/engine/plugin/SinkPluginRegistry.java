package com.fanout.engine.plugin;

import com.fanout.engine.config.SinkType;

import java.util.EnumMap;
import java.util.Map;

public class SinkPluginRegistry {
    private final Map<SinkType, SinkPlugin<?>> plugins = new EnumMap<>(SinkType.class);

    public SinkPluginRegistry() {
        register(new RestSinkPlugin());
        register(new GrpcSinkPlugin());
        register(new MessageQueueSinkPlugin());
        register(new WideColumnSinkPlugin());
    }

    public void register(SinkPlugin<?> plugin) {
        plugins.put(plugin.type(), plugin);
    }

    public SinkPlugin<?> get(SinkType sinkType) {
        SinkPlugin<?> plugin = plugins.get(sinkType);
        if (plugin == null) {
            throw new IllegalArgumentException("No plugin registered for sink type " + sinkType);
        }
        return plugin;
    }
}
