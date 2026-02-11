package com.fanout.engine.config;

public class EngineConfig {
    private boolean useVirtualThreads = true;
    private int defaultWorkersPerSink = Math.max(2, Runtime.getRuntime().availableProcessors());
    private int queueCapacity = 10_000;
    private int maxRetries = 3;
    private long retryBackoffMillis = 50;
    private int shutdownTimeoutSeconds = 30;
    private String deadLetterPath = "./build/dlq/dead-letter.jsonl";

    public boolean isUseVirtualThreads() {
        return useVirtualThreads;
    }

    public void setUseVirtualThreads(boolean useVirtualThreads) {
        this.useVirtualThreads = useVirtualThreads;
    }

    public int getDefaultWorkersPerSink() {
        return defaultWorkersPerSink;
    }

    public void setDefaultWorkersPerSink(int defaultWorkersPerSink) {
        this.defaultWorkersPerSink = defaultWorkersPerSink;
    }

    public int getQueueCapacity() {
        return queueCapacity;
    }

    public void setQueueCapacity(int queueCapacity) {
        this.queueCapacity = queueCapacity;
    }

    public int getMaxRetries() {
        return maxRetries;
    }

    public void setMaxRetries(int maxRetries) {
        this.maxRetries = maxRetries;
    }

    public long getRetryBackoffMillis() {
        return retryBackoffMillis;
    }

    public void setRetryBackoffMillis(long retryBackoffMillis) {
        this.retryBackoffMillis = retryBackoffMillis;
    }

    public int getShutdownTimeoutSeconds() {
        return shutdownTimeoutSeconds;
    }

    public void setShutdownTimeoutSeconds(int shutdownTimeoutSeconds) {
        this.shutdownTimeoutSeconds = shutdownTimeoutSeconds;
    }

    public String getDeadLetterPath() {
        return deadLetterPath;
    }

    public void setDeadLetterPath(String deadLetterPath) {
        this.deadLetterPath = deadLetterPath;
    }
}
