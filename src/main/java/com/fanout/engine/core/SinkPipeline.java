package com.fanout.engine.core;

import com.fanout.engine.config.SinkConfig;
import com.fanout.engine.model.RecordEnvelope;
import com.fanout.engine.sink.Sink;
import com.fanout.engine.throttle.RateLimiter;
import com.fanout.engine.transform.Transformer;

import java.util.concurrent.BlockingQueue;

public final class SinkPipeline<T> {
    private final SinkConfig config;
    private final Transformer<T> transformer;
    private final Sink<T> sink;
    private final BlockingQueue<RecordEnvelope> queue;
    private final RateLimiter rateLimiter;
    private final int workers;

    public SinkPipeline(SinkConfig config,
                        Transformer<T> transformer,
                        Sink<T> sink,
                        BlockingQueue<RecordEnvelope> queue,
                        RateLimiter rateLimiter,
                        int workers) {
        this.config = config;
        this.transformer = transformer;
        this.sink = sink;
        this.queue = queue;
        this.rateLimiter = rateLimiter;
        this.workers = workers;
    }

    public SinkConfig config() {
        return config;
    }

    public Transformer<T> transformer() {
        return transformer;
    }

    public Sink<T> sink() {
        return sink;
    }

    public BlockingQueue<RecordEnvelope> queue() {
        return queue;
    }

    public RateLimiter rateLimiter() {
        return rateLimiter;
    }

    public int workers() {
        return workers;
    }
}
