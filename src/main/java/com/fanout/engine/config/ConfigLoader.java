package com.fanout.engine.config;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public final class ConfigLoader {
    private ConfigLoader() {
    }

    public static AppConfig load(Path configPath) throws IOException {
        if (!Files.exists(configPath)) {
            throw new IllegalArgumentException("Config file does not exist: " + configPath);
        }

        String name = configPath.getFileName().toString().toLowerCase();
        ObjectMapper mapper;
        if (name.endsWith(".yaml") || name.endsWith(".yml")) {
            mapper = new ObjectMapper(new YAMLFactory());
        } else if (name.endsWith(".json")) {
            mapper = new ObjectMapper();
        } else {
            throw new IllegalArgumentException("Unsupported config format. Use .yaml/.yml or .json: " + configPath);
        }

        mapper.findAndRegisterModules();
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

        AppConfig config = mapper.readValue(configPath.toFile(), AppConfig.class);
        config.validate();
        return config;
    }
}
