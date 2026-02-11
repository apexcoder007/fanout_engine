package com.fanout.engine.model;

import java.time.Instant;
import java.util.Map;

public final class DeadLetterEntry {
    private final Instant timestamp;
    private final long recordSequence;
    private final String sinkName;
    private final int attempts;
    private final String error;
    private final Map<String, Object> fields;

    public DeadLetterEntry(Instant timestamp,
                           long recordSequence,
                           String sinkName,
                           int attempts,
                           String error,
                           Map<String, Object> fields) {
        this.timestamp = timestamp;
        this.recordSequence = recordSequence;
        this.sinkName = sinkName;
        this.attempts = attempts;
        this.error = error;
        this.fields = fields;
    }

    public Instant getTimestamp() {
        return timestamp;
    }

    public long getRecordSequence() {
        return recordSequence;
    }

    public String getSinkName() {
        return sinkName;
    }

    public int getAttempts() {
        return attempts;
    }

    public String getError() {
        return error;
    }

    public Map<String, Object> getFields() {
        return fields;
    }
}
