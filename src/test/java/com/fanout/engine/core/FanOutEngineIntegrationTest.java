package com.fanout.engine.core;

import com.fanout.engine.config.AppConfig;
import com.fanout.engine.config.EngineConfig;
import com.fanout.engine.config.InputConfig;
import com.fanout.engine.config.InputType;
import com.fanout.engine.config.ObservabilityConfig;
import com.fanout.engine.config.SinkConfig;
import com.fanout.engine.config.SinkType;
import com.fanout.engine.ingest.InputReaderFactory;
import com.fanout.engine.plugin.SinkPlugin;
import com.fanout.engine.plugin.SinkPluginRegistry;
import com.fanout.engine.sink.Sink;
import com.fanout.engine.transform.Transformer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class FanOutEngineIntegrationTest {

    @TempDir
    Path tempDir;

    @Test
    void shouldDeliverAllRecordsToSinkViaOrchestrator() throws Exception {
        Path inputPath = tempDir.resolve("input.csv");
        Files.writeString(inputPath, "id,name\n1,alice\n2,bob\n");

        Path dlqPath = tempDir.resolve("dlq.jsonl");

        Transformer<String> transformer = mockStringTransformer();
        Sink<String> sink = mockStringSink();
        when(transformer.transform(any())).thenReturn("payload");
        doNothing().when(sink).send(anyString());

        SinkPluginRegistry registry = registryWithRestOverride(transformer, sink);
        FanOutEngine engine = new FanOutEngine(buildConfig(inputPath, dlqPath), registry, new InputReaderFactory());
        EngineRunResult result = engine.run();

        assertEquals(2, result.recordsIngested());
        assertEquals(2, result.expectedDeliveries());
        assertEquals(2, result.accountedDeliveries());
        verify(sink, times(2)).send(anyString());
    }

    @Test
    void shouldRetryThreeTimesThenDeadLetter() throws Exception {
        Path inputPath = tempDir.resolve("input.csv");
        Files.writeString(inputPath, "id,name\n1,alice\n");

        Path dlqPath = tempDir.resolve("dlq.jsonl");

        Transformer<String> transformer = mockStringTransformer();
        Sink<String> sink = mockStringSink();
        when(transformer.transform(any())).thenReturn("payload");
        doThrow(new RuntimeException("downstream unavailable")).when(sink).send(anyString());

        SinkPluginRegistry registry = registryWithRestOverride(transformer, sink);
        AppConfig config = buildConfig(inputPath, dlqPath);
        config.getEngine().setRetryBackoffMillis(1);
        FanOutEngine engine = new FanOutEngine(config, registry, new InputReaderFactory());

        EngineRunResult result = engine.run();

        assertEquals(1, result.recordsIngested());
        assertEquals(1, result.expectedDeliveries());
        assertEquals(1, result.accountedDeliveries());
        verify(sink, times(4)).send(anyString());

        List<String> dlqLines = Files.readAllLines(dlqPath);
        assertEquals(1, dlqLines.size());
    }

    private SinkPluginRegistry registryWithRestOverride(Transformer<String> transformer, Sink<String> sink) {
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
        return registry;
    }

    @SuppressWarnings("unchecked")
    private Transformer<String> mockStringTransformer() {
        return (Transformer<String>) mock(Transformer.class);
    }

    @SuppressWarnings("unchecked")
    private Sink<String> mockStringSink() {
        return (Sink<String>) mock(Sink.class);
    }

    private AppConfig buildConfig(Path inputPath, Path dlqPath) {
        InputConfig inputConfig = new InputConfig();
        inputConfig.setType(InputType.CSV);
        inputConfig.setPath(inputPath.toString());
        inputConfig.setDelimiter(",");
        inputConfig.setHasHeader(true);

        SinkConfig sinkConfig = new SinkConfig();
        sinkConfig.setName("rest-test");
        sinkConfig.setType(SinkType.REST_API);
        sinkConfig.setRateLimitPerSecond(10_000);
        sinkConfig.setWorkers(1);
        sinkConfig.setQueueCapacity(32);
        sinkConfig.setFailureRate(0);
        sinkConfig.setMinLatencyMs(0);
        sinkConfig.setMaxLatencyMs(0);

        EngineConfig engineConfig = new EngineConfig();
        engineConfig.setUseVirtualThreads(false);
        engineConfig.setDefaultWorkersPerSink(1);
        engineConfig.setQueueCapacity(32);
        engineConfig.setMaxRetries(3);
        engineConfig.setRetryBackoffMillis(5);
        engineConfig.setDeadLetterPath(dlqPath.toString());

        ObservabilityConfig observabilityConfig = new ObservabilityConfig();
        observabilityConfig.setStatusIntervalSeconds(1);

        AppConfig appConfig = new AppConfig();
        appConfig.setInput(inputConfig);
        appConfig.setEngine(engineConfig);
        appConfig.setObservability(observabilityConfig);
        appConfig.setSinks(List.of(sinkConfig));
        appConfig.validate();
        return appConfig;
    }
}
