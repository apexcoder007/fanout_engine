package com.fanout.engine.config;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ConfigLoaderTest {

    @TempDir
    Path tempDir;

    @Test
    void shouldLoadYamlConfig() throws Exception {
        Path configPath = tempDir.resolve("app.yaml");
        Files.writeString(configPath, """
                input:
                  type: csv
                  path: samples/customers.csv
                engine:
                  defaultWorkersPerSink: 2
                observability:
                  statusIntervalSeconds: 5
                sinks:
                  - name: rest
                    type: REST_API
                    rateLimitPerSecond: 10
                    workers: 1
                    queueCapacity: 32
                """);

        AppConfig config = ConfigLoader.load(configPath);

        assertEquals(InputType.CSV, config.getInput().getType());
        assertEquals("samples/customers.csv", config.getInput().getPath());
        assertEquals(1, config.getSinks().size());
        assertEquals(SinkType.REST_API, config.getSinks().getFirst().getType());
    }

    @Test
    void shouldLoadJsonConfig() throws Exception {
        Path configPath = tempDir.resolve("app.json");
        Files.writeString(configPath, """
                {
                  "input": {
                    "type": "jsonl",
                    "path": "samples/customers.jsonl"
                  },
                  "engine": {
                    "defaultWorkersPerSink": 2
                  },
                  "observability": {
                    "statusIntervalSeconds": 5
                  },
                  "sinks": [
                    {
                      "name": "mq",
                      "type": "MESSAGE_QUEUE",
                      "rateLimitPerSecond": 20,
                      "workers": 1,
                      "queueCapacity": 16
                    }
                  ]
                }
                """);

        AppConfig config = ConfigLoader.load(configPath);

        assertEquals(InputType.JSONL, config.getInput().getType());
        assertEquals(SinkType.MESSAGE_QUEUE, config.getSinks().getFirst().getType());
    }

    @Test
    void shouldFailForMissingConfigFile() {
        Path missing = tempDir.resolve("missing.yaml");
        assertThrows(IllegalArgumentException.class, () -> ConfigLoader.load(missing));
    }

    @Test
    void shouldFailForUnsupportedExtension() throws Exception {
        Path configPath = tempDir.resolve("app.txt");
        Files.writeString(configPath, "dummy");

        assertThrows(IllegalArgumentException.class, () -> ConfigLoader.load(configPath));
    }
}
