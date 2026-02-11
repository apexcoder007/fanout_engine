package com.fanout.engine.sink;

import com.fanout.engine.config.SinkType;

public interface Sink<T> extends AutoCloseable {
    String name();

    SinkType type();

    void send(T payload) throws Exception;

    @Override
    default void close() throws Exception {
    }
}
