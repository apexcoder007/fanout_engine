package com.fanout.engine.model;

public final class RecordEnvelope {
    private static final RecordEnvelope POISON = new RecordEnvelope(true, null);

    private final boolean poison;
    private final SourceRecord record;

    private RecordEnvelope(boolean poison, SourceRecord record) {
        this.poison = poison;
        this.record = record;
    }

    public static RecordEnvelope fromRecord(SourceRecord record) {
        return new RecordEnvelope(false, record);
    }

    public static RecordEnvelope poison() {
        return POISON;
    }

    public boolean isPoison() {
        return poison;
    }

    public SourceRecord record() {
        return record;
    }
}
