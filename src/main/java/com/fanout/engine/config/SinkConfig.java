package com.fanout.engine.config;

import java.util.HashMap;
import java.util.Map;

public class SinkConfig {
    private String name;
    private SinkType type;
    private boolean enabled = true;
    private String endpoint;
    private double rateLimitPerSecond = 50.0;
    private int workers = 0;
    private int queueCapacity = 1000;
    private int minLatencyMs = 5;
    private int maxLatencyMs = 50;
    private double failureRate = 0.02;
    private Map<String, String> options = new HashMap<>();

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public SinkType getType() {
        return type;
    }

    public void setType(SinkType type) {
        this.type = type;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public void setEndpoint(String endpoint) {
        this.endpoint = endpoint;
    }

    public double getRateLimitPerSecond() {
        return rateLimitPerSecond;
    }

    public void setRateLimitPerSecond(double rateLimitPerSecond) {
        this.rateLimitPerSecond = rateLimitPerSecond;
    }

    public int getWorkers() {
        return workers;
    }

    public void setWorkers(int workers) {
        this.workers = workers;
    }

    public int getQueueCapacity() {
        return queueCapacity;
    }

    public void setQueueCapacity(int queueCapacity) {
        this.queueCapacity = queueCapacity;
    }

    public int getMinLatencyMs() {
        return minLatencyMs;
    }

    public void setMinLatencyMs(int minLatencyMs) {
        this.minLatencyMs = minLatencyMs;
    }

    public int getMaxLatencyMs() {
        return maxLatencyMs;
    }

    public void setMaxLatencyMs(int maxLatencyMs) {
        this.maxLatencyMs = maxLatencyMs;
    }

    public double getFailureRate() {
        return failureRate;
    }

    public void setFailureRate(double failureRate) {
        this.failureRate = failureRate;
    }

    public Map<String, String> getOptions() {
        return options;
    }

    public void setOptions(Map<String, String> options) {
        this.options = options;
    }
}
