package com.fanout.engine.model;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

public final class WideColumnPayload {
    private final byte[] avroBytes;
    private final Map<String, Object> cqlMap;

    public WideColumnPayload(byte[] avroBytes, Map<String, Object> cqlMap) {
        this.avroBytes = avroBytes;
        this.cqlMap = Collections.unmodifiableMap(new LinkedHashMap<>(cqlMap));
    }

    public byte[] avroBytes() {
        return avroBytes;
    }

    public Map<String, Object> cqlMap() {
        return cqlMap;
    }
}
