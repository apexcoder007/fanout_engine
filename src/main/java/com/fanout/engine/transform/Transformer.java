package com.fanout.engine.transform;

import com.fanout.engine.model.SourceRecord;

@FunctionalInterface
public interface Transformer<T> {
    T transform(SourceRecord record) throws Exception;
}
