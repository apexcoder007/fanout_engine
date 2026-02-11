package com.fanout.engine.config;

public class ObservabilityConfig {
    private int statusIntervalSeconds = 5;

    public int getStatusIntervalSeconds() {
        return statusIntervalSeconds;
    }

    public void setStatusIntervalSeconds(int statusIntervalSeconds) {
        this.statusIntervalSeconds = statusIntervalSeconds;
    }
}
