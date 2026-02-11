package com.fanout.engine.config;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class AppConfigValidationTest {

    @Test
    void shouldValidateMinimalConfig() {
        AppConfig config = validConfig();
        assertDoesNotThrow(config::validate);
    }

    @Test
    void shouldDefaultEngineAndObservabilityWhenNull() {
        AppConfig config = validConfig();
        config.setEngine(null);
        config.setObservability(null);

        config.validate();

        assertNotNull(config.getEngine());
        assertNotNull(config.getObservability());
    }

    @Test
    void shouldFailWhenInputPathMissing() {
        AppConfig config = validConfig();
        config.getInput().setPath("   ");
        assertThrows(IllegalArgumentException.class, config::validate);
    }

    @Test
    void shouldFailWhenNoSinksConfigured() {
        AppConfig config = validConfig();
        config.setSinks(List.of());
        assertThrows(IllegalArgumentException.class, config::validate);
    }

    @Test
    void shouldFailOnInvalidEngineSettings() {
        AppConfig config = validConfig();
        config.getEngine().setMaxRetries(-1);
        assertThrows(IllegalArgumentException.class, config::validate);

        config = validConfig();
        config.getEngine().setRetryBackoffMillis(-1);
        assertThrows(IllegalArgumentException.class, config::validate);

        config = validConfig();
        config.getEngine().setDefaultWorkersPerSink(0);
        assertThrows(IllegalArgumentException.class, config::validate);
    }

    @Test
    void shouldFailOnInvalidObservabilityInterval() {
        AppConfig config = validConfig();
        config.getObservability().setStatusIntervalSeconds(0);
        assertThrows(IllegalArgumentException.class, config::validate);
    }

    @Test
    void shouldFailOnInvalidSinkSettings() {
        AppConfig config = validConfig();
        config.getSinks().getFirst().setName(" ");
        assertThrows(IllegalArgumentException.class, config::validate);

        config = validConfig();
        config.getSinks().getFirst().setType(null);
        assertThrows(IllegalArgumentException.class, config::validate);

        config = validConfig();
        config.getSinks().getFirst().setRateLimitPerSecond(0);
        assertThrows(IllegalArgumentException.class, config::validate);

        config = validConfig();
        config.getSinks().getFirst().setWorkers(-1);
        assertThrows(IllegalArgumentException.class, config::validate);
    }

    @Test
    void shouldBackfillSinkQueueCapacityFromEngineWhenNonPositive() {
        AppConfig config = validConfig();
        config.getEngine().setQueueCapacity(777);
        config.getSinks().getFirst().setQueueCapacity(0);

        config.validate();

        assertEquals(777, config.getSinks().getFirst().getQueueCapacity());
    }

    private AppConfig validConfig() {
        InputConfig inputConfig = new InputConfig();
        inputConfig.setType(InputType.CSV);
        inputConfig.setPath("samples/customers.csv");

        SinkConfig sinkConfig = new SinkConfig();
        sinkConfig.setName("rest");
        sinkConfig.setType(SinkType.REST_API);
        sinkConfig.setRateLimitPerSecond(1);
        sinkConfig.setWorkers(1);
        sinkConfig.setQueueCapacity(8);

        AppConfig config = new AppConfig();
        config.setInput(inputConfig);
        config.setEngine(new EngineConfig());
        config.setObservability(new ObservabilityConfig());
        config.setSinks(List.of(sinkConfig));
        return config;
    }
}
