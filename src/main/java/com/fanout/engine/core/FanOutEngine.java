package com.fanout.engine.core;

import com.fanout.engine.config.AppConfig;
import com.fanout.engine.config.EngineConfig;
import com.fanout.engine.config.SinkConfig;
import com.fanout.engine.ingest.InputReader;
import com.fanout.engine.ingest.InputReaderFactory;
import com.fanout.engine.metrics.MetricsCollector;
import com.fanout.engine.model.RecordEnvelope;
import com.fanout.engine.model.SourceRecord;
import com.fanout.engine.plugin.SinkPlugin;
import com.fanout.engine.plugin.SinkPluginRegistry;
import com.fanout.engine.resilience.DeadLetterQueue;
import com.fanout.engine.sink.Sink;
import com.fanout.engine.throttle.TokenBucketRateLimiter;
import com.fanout.engine.transform.Transformer;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public final class FanOutEngine {
    private final AppConfig config;
    private final SinkPluginRegistry pluginRegistry;
    private final InputReaderFactory inputReaderFactory;

    public FanOutEngine(AppConfig config) {
        this(config, new SinkPluginRegistry(), new InputReaderFactory());
    }

    public FanOutEngine(AppConfig config,
                        SinkPluginRegistry pluginRegistry,
                        InputReaderFactory inputReaderFactory) {
        this.config = config;
        this.pluginRegistry = pluginRegistry;
        this.inputReaderFactory = inputReaderFactory;
    }

    public EngineRunResult run() throws Exception {
        EngineConfig engineConfig = config.getEngine();

        MetricsCollector metrics = new MetricsCollector();
        List<SinkPipeline<?>> pipelines = createPipelines(config, engineConfig, metrics);
        if (pipelines.isEmpty()) {
            throw new IllegalArgumentException("No enabled sinks configured");
        }

        try (DeadLetterQueue deadLetterQueue = new DeadLetterQueue(Path.of(engineConfig.getDeadLetterPath()))) {
            ExecutorService workerExecutor = createWorkerExecutor(engineConfig, pipelines);
            ScheduledExecutorService reporter = Executors.newSingleThreadScheduledExecutor();

            List<Future<?>> workerFutures = new ArrayList<>();
            boolean poisonQueued = false;

            try {
                reporter.scheduleAtFixedRate(
                        () -> System.out.println(metrics.formatStatusLine()),
                        config.getObservability().getStatusIntervalSeconds(),
                        config.getObservability().getStatusIntervalSeconds(),
                        TimeUnit.SECONDS
                );

                for (SinkPipeline<?> pipeline : pipelines) {
                    for (int i = 0; i < pipeline.workers(); i++) {
                        workerFutures.add(workerExecutor.submit(createWorker(pipeline, metrics, deadLetterQueue, engineConfig)));
                    }
                }

                InputReader reader = inputReaderFactory.create(config.getInput());
                reader.stream(record -> {
                    metrics.markIngestedRecord();
                    dispatchRecord(record, pipelines);
                });

                enqueuePoisonPills(pipelines);
                poisonQueued = true;
                waitForWorkers(workerFutures);
            } finally {
                if (!poisonQueued) {
                    enqueuePoisonPills(pipelines);
                }
                reporter.shutdownNow();
                workerExecutor.shutdown();
                workerExecutor.awaitTermination(engineConfig.getShutdownTimeoutSeconds(), TimeUnit.SECONDS);
                closePipelines(pipelines);
            }

            long ingested = metrics.ingestedCount();
            long expectedDeliveries = ingested * pipelines.size();
            long accountedDeliveries = metrics.totalAccountedDeliveries();

            if (expectedDeliveries != accountedDeliveries) {
                throw new IllegalStateException("Zero data loss violated. expectedDeliveries="
                        + expectedDeliveries + " accountedDeliveries=" + accountedDeliveries);
            }

            return new EngineRunResult(ingested, expectedDeliveries, accountedDeliveries);
        }
    }

    private ExecutorService createWorkerExecutor(EngineConfig engineConfig, List<SinkPipeline<?>> pipelines) {
        int workerCount = pipelines.stream().mapToInt(SinkPipeline::workers).sum();
        if (engineConfig.isUseVirtualThreads()) {
            return Executors.newVirtualThreadPerTaskExecutor();
        }
        return Executors.newFixedThreadPool(workerCount);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private Runnable createWorker(SinkPipeline<?> pipeline,
                                  MetricsCollector metrics,
                                  DeadLetterQueue deadLetterQueue,
                                  EngineConfig engineConfig) {
        return new SinkWorker((SinkPipeline) pipeline,
                metrics,
                deadLetterQueue,
                engineConfig.getMaxRetries(),
                engineConfig.getRetryBackoffMillis());
    }

    private void dispatchRecord(SourceRecord record, List<SinkPipeline<?>> pipelines) {
        for (SinkPipeline<?> pipeline : pipelines) {
            try {
                pipeline.queue().put(RecordEnvelope.fromRecord(record));
            } catch (InterruptedException interruptedException) {
                Thread.currentThread().interrupt();
                throw new CompletionException(interruptedException);
            }
        }
    }

    private void enqueuePoisonPills(List<SinkPipeline<?>> pipelines) {
        for (SinkPipeline<?> pipeline : pipelines) {
            for (int i = 0; i < pipeline.workers(); i++) {
                boolean enqueued = false;
                for (int attempt = 0; attempt < 30; attempt++) {
                    try {
                        if (pipeline.queue().offer(RecordEnvelope.poison(), 1, TimeUnit.SECONDS)) {
                            enqueued = true;
                            break;
                        }
                    } catch (InterruptedException interruptedException) {
                        Thread.currentThread().interrupt();
                        throw new CompletionException(interruptedException);
                    }
                }
                if (!enqueued) {
                    throw new IllegalStateException("Failed to enqueue poison pill for sink " + pipeline.config().getName());
                }
            }
        }
    }

    private void waitForWorkers(List<Future<?>> workerFutures) throws Exception {
        for (Future<?> future : workerFutures) {
            future.get();
        }
    }

    private void closePipelines(List<SinkPipeline<?>> pipelines) throws Exception {
        for (SinkPipeline<?> pipeline : pipelines) {
            pipeline.sink().close();
        }
    }

    private List<SinkPipeline<?>> createPipelines(AppConfig appConfig,
                                                  EngineConfig engineConfig,
                                                  MetricsCollector metrics) {
        List<SinkPipeline<?>> pipelines = new ArrayList<>();

        for (SinkConfig sinkConfig : appConfig.getSinks()) {
            if (!sinkConfig.isEnabled()) {
                continue;
            }

            SinkPipeline<?> pipeline = buildPipeline(sinkConfig, engineConfig);
            metrics.registerSink(sinkConfig.getName());
            pipelines.add(pipeline);
        }

        return pipelines;
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private SinkPipeline<?> buildPipeline(SinkConfig sinkConfig, EngineConfig engineConfig) {
        SinkPlugin plugin = pluginRegistry.get(sinkConfig.getType());
        Transformer transformer = plugin.createTransformer();
        Sink sink = plugin.createSink(sinkConfig);

        int queueCapacity = sinkConfig.getQueueCapacity() > 0
                ? sinkConfig.getQueueCapacity()
                : engineConfig.getQueueCapacity();
        BlockingQueue<RecordEnvelope> queue = new ArrayBlockingQueue<>(queueCapacity);
        int workers = sinkConfig.getWorkers() > 0
                ? sinkConfig.getWorkers()
                : engineConfig.getDefaultWorkersPerSink();

        return new SinkPipeline<>(
                sinkConfig,
                transformer,
                sink,
                queue,
                new TokenBucketRateLimiter(sinkConfig.getRateLimitPerSecond()),
                workers
        );
    }
}
