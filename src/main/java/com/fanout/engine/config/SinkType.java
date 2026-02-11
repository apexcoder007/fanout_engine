package com.fanout.engine.config;

import com.fasterxml.jackson.annotation.JsonCreator;

public enum SinkType {
    REST_API,
    GRPC,
    MESSAGE_QUEUE,
    WIDE_COLUMN_DB;

    @JsonCreator
    public static SinkType fromValue(String value) {
        if (value == null) {
            throw new IllegalArgumentException("Sink type cannot be null");
        }
        String normalized = value.trim().replace('-', '_').toUpperCase();
        return switch (normalized) {
            case "REST", "REST_API", "HTTP", "HTTP2" -> REST_API;
            case "GRPC" -> GRPC;
            case "MQ", "MESSAGE_QUEUE", "KAFKA", "RABBITMQ" -> MESSAGE_QUEUE;
            case "WIDE_COLUMN", "WIDE_COLUMN_DB", "CASSANDRA", "NOSQL" -> WIDE_COLUMN_DB;
            default -> throw new IllegalArgumentException("Unsupported sink type: " + value);
        };
    }
}
