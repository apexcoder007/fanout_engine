package com.fanout.engine.config;

import com.fasterxml.jackson.annotation.JsonCreator;

public enum InputType {
    CSV,
    JSONL,
    FIXED_WIDTH;

    @JsonCreator
    public static InputType fromValue(String value) {
        if (value == null) {
            return CSV;
        }
        String normalized = value.trim().replace('-', '_').toUpperCase();
        return switch (normalized) {
            case "CSV" -> CSV;
            case "JSONL", "JSON_LINES" -> JSONL;
            case "FIXED_WIDTH", "FIXEDWIDTH" -> FIXED_WIDTH;
            default -> throw new IllegalArgumentException("Unsupported input type: " + value);
        };
    }
}
