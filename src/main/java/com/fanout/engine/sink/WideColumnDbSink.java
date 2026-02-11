package com.fanout.engine.sink;

import com.fanout.engine.config.SinkConfig;
import com.fanout.engine.config.SinkType;
import com.fanout.engine.model.WideColumnPayload;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public final class WideColumnDbSink extends AbstractMockSink<WideColumnPayload> {
    private final ExecutorService asyncExecutor = Executors.newFixedThreadPool(2);

    public WideColumnDbSink(SinkConfig sinkConfig) {
        super(sinkConfig);
    }

    @Override
    public SinkType type() {
        return SinkType.WIDE_COLUMN_DB;
    }

    @Override
    public void send(WideColumnPayload payload) throws Exception {
        if (payload == null || payload.avroBytes() == null || payload.avroBytes().length == 0) {
            throw new IllegalArgumentException("Wide-column payload cannot be empty");
        }

        CompletableFuture<Void> upsert = CompletableFuture.runAsync(() -> {
            try {
                simulateNetworkOrIo();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, asyncExecutor);

        try {
            upsert.get();
        } catch (ExecutionException executionException) {
            Throwable cause = executionException.getCause();
            if (cause instanceof Exception ex) {
                throw ex;
            }
            throw executionException;
        }
    }

    @Override
    public void close() throws Exception {
        asyncExecutor.shutdown();
        if (!asyncExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
            asyncExecutor.shutdownNow();
        }
    }
}
