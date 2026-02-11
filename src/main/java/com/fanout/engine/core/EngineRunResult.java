package com.fanout.engine.core;

public record EngineRunResult(long recordsIngested, long expectedDeliveries, long accountedDeliveries) {
}
