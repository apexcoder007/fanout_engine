package com.fanout.engine.model;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

public final class SourceRecord {
    private final long sequence;
    private final Map<String, Object> fields;
    private final String rawLine;

    public SourceRecord(long sequence, Map<String, Object> fields, String rawLine) {
        this.sequence = sequence;
        this.fields = Collections.unmodifiableMap(new LinkedHashMap<>(fields));
        this.rawLine = rawLine;
    }

    public long sequence() {
        return sequence;
    }

    public Map<String, Object> fields() {
        return fields;
    }

    public String rawLine() {
        return rawLine;
    }
}
