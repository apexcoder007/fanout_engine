package com.fanout.engine.core;

import com.fanout.engine.config.AppConfig;
import com.fanout.engine.config.EngineConfig;
import com.fanout.engine.config.InputConfig;
import com.fanout.engine.config.InputType;
import com.fanout.engine.config.ObservabilityConfig;
import com.fanout.engine.config.SinkConfig;
import com.fanout.engine.config.SinkType;
import com.fanout.engine.ingest.InputReaderFactory;
import com.fanout.engine.model.SourceRecord;
import com.fanout.engine.plugin.SinkPlugin;
import com.fanout.engine.plugin.SinkPluginRegistry;
import com.fanout.engine.sink.Sink;
import com.fanout.engine.transform.Transformer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class FanOutEngineEdgeCaseIntegrationTest {

    @TempDir
    Path tempDir;

    @Test
    void shouldFailFastWhenAllSinksDisabled() throws Exception {
        Path inputPath = tempDir.resolve("input.csv");
        Files.writeString(inputPath, "id,name\n1,alice\n");

        SinkConfig sinkConfig = restSink("rest-disabled");
        sinkConfig.setEnabled(false);

        AppConfig config = appConfig(inputPath, tempDir.resolve("dlq.jsonl"), List.of(sinkConfig));

        FanOutEngine engine = new FanOutEngine(config, new SinkPluginRegistry(), new InputReaderFactory());

        assertThrows(IllegalArgumentException.class, engine::run);
    }

    @Test
    void shouldStreamLargeInputWithSmallQueueWithoutDataLoss() throws Exception {
        Path inputPath = tempDir.resolve("large.csv");
        StringBuilder builder = new StringBuilder("id,name\n");
        int records = 3_000;
        for (int i = 1; i <= records; i++) {
            builder.append(i).append(",user").append(i).append('\n');
        }
        Files.writeString(inputPath, builder.toString());

        AtomicInteger sentCount = new AtomicInteger();
        Sink<String> sink = new Sink<>() {
            @Override
            public String name() {
                return "counting-rest-sink";
            }

            @Override
            public SinkType type() {
                return SinkType.REST_API;
            }

            @Override
            public void send(String payload) {
                sentCount.incrementAndGet();
            }
        };
        Transformer<String> transformer = SourceRecord::rawLine;

        SinkPluginRegistry registry = new SinkPluginRegistry();
        registry.register(new SinkPlugin<String>() {
            @Override
            public SinkType type() {
                return SinkType.REST_API;
            }

            @Override
            public Transformer<String> createTransformer() {
                return transformer;
            }

            @Override
            public Sink<String> createSink(SinkConfig config) {
                return sink;
            }
        });

        SinkConfig sinkConfig = restSink("rest-stream");
        sinkConfig.setQueueCapacity(8);
        sinkConfig.setWorkers(2);

        AppConfig config = appConfig(inputPath, tempDir.resolve("dlq.jsonl"), List.of(sinkConfig));
        EngineRunResult result = new FanOutEngine(config, registry, new InputReaderFactory()).run();

        assertEquals(records, result.recordsIngested());
        assertEquals(records, result.expectedDeliveries());
        assertEquals(records, result.accountedDeliveries());
        assertEquals(records, sentCount.get());
    }

    private AppConfig appConfig(Path inputPath, Path dlqPath, List<SinkConfig> sinks) {
        InputConfig inputConfig = new InputConfig();
        inputConfig.setType(InputType.CSV);
        inputConfig.setPath(inputPath.toString());
        inputConfig.setDelimiter(",");
        inputConfig.setHasHeader(true);

        EngineConfig engineConfig = new EngineConfig();
        engineConfig.setUseVirtualThreads(false);
        engineConfig.setDefaultWorkersPerSink(2);
        engineConfig.setQueueCapacity(16);
        engineConfig.setMaxRetries(1);
        engineConfig.setRetryBackoffMillis(1);
        engineConfig.setDeadLetterPath(dlqPath.toString());

        ObservabilityConfig observabilityConfig = new ObservabilityConfig();
        observabilityConfig.setStatusIntervalSeconds(60);

        AppConfig appConfig = new AppConfig();
        appConfig.setInput(inputConfig);
        appConfig.setEngine(engineConfig);
        appConfig.setObservability(observabilityConfig);
        appConfig.setSinks(sinks);
        appConfig.validate();
        return appConfig;
    }

    private SinkConfig restSink(String name) {
        SinkConfig sinkConfig = new SinkConfig();
        sinkConfig.setName(name);
        sinkConfig.setType(SinkType.REST_API);
        sinkConfig.setRateLimitPerSecond(100_000);
        sinkConfig.setWorkers(1);
        sinkConfig.setQueueCapacity(16);
        sinkConfig.setFailureRate(0);
        sinkConfig.setMinLatencyMs(0);
        sinkConfig.setMaxLatencyMs(0);
        return sinkConfig;
    }
}
