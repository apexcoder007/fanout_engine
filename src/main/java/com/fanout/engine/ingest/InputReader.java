package com.fanout.engine.ingest;

import com.fanout.engine.model.SourceRecord;

import java.io.IOException;
import java.util.function.Consumer;

public interface InputReader {
    void stream(Consumer<SourceRecord> consumer) throws IOException;
}
