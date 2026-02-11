package com.fanout.engine.config;

import java.util.ArrayList;
import java.util.List;

public class AppConfig {
    private InputConfig input = new InputConfig();
    private EngineConfig engine = new EngineConfig();
    private ObservabilityConfig observability = new ObservabilityConfig();
    private List<SinkConfig> sinks = new ArrayList<>();

    public InputConfig getInput() {
        return input;
    }

    public void setInput(InputConfig input) {
        this.input = input;
    }

    public EngineConfig getEngine() {
        return engine;
    }

    public void setEngine(EngineConfig engine) {
        this.engine = engine;
    }

    public ObservabilityConfig getObservability() {
        return observability;
    }

    public void setObservability(ObservabilityConfig observability) {
        this.observability = observability;
    }

    public List<SinkConfig> getSinks() {
        return sinks;
    }

    public void setSinks(List<SinkConfig> sinks) {
        this.sinks = sinks;
    }

    public void validate() {
        if (engine == null) {
            engine = new EngineConfig();
        }
        if (observability == null) {
            observability = new ObservabilityConfig();
        }
        if (input == null) {
            throw new IllegalArgumentException("input config is required");
        }
        if (input.getPath() == null || input.getPath().isBlank()) {
            throw new IllegalArgumentException("input.path is required");
        }
        if (observability.getStatusIntervalSeconds() <= 0) {
            throw new IllegalArgumentException("observability.statusIntervalSeconds must be > 0");
        }
        if (engine.getMaxRetries() < 0) {
            throw new IllegalArgumentException("engine.maxRetries must be >= 0");
        }
        if (engine.getRetryBackoffMillis() < 0) {
            throw new IllegalArgumentException("engine.retryBackoffMillis must be >= 0");
        }
        if (engine.getDefaultWorkersPerSink() <= 0) {
            throw new IllegalArgumentException("engine.defaultWorkersPerSink must be > 0");
        }
        if (sinks == null || sinks.isEmpty()) {
            throw new IllegalArgumentException("At least one sink config is required");
        }
        for (SinkConfig sink : sinks) {
            if (sink.getName() == null || sink.getName().isBlank()) {
                throw new IllegalArgumentException("sink.name is required for all sinks");
            }
            if (sink.getType() == null) {
                throw new IllegalArgumentException("sink.type is required for sink " + sink.getName());
            }
            if (sink.getRateLimitPerSecond() <= 0) {
                throw new IllegalArgumentException("sink.rateLimitPerSecond must be > 0 for sink " + sink.getName());
            }
            if (sink.getQueueCapacity() <= 0) {
                sink.setQueueCapacity(engine.getQueueCapacity());
            }
            if (sink.getWorkers() < 0) {
                throw new IllegalArgumentException("sink.workers must be >= 0 for sink " + sink.getName());
            }
        }
    }
}
