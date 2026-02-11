package com.fanout.engine.ingest;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fanout.engine.config.InputConfig;
import com.fanout.engine.config.InputType;

import java.nio.file.Path;

public final class InputReaderFactory {
    private final ObjectMapper objectMapper;

    public InputReaderFactory() {
        this.objectMapper = new ObjectMapper().findAndRegisterModules();
    }

    public InputReader create(InputConfig config) {
        Path path = Path.of(config.getPath());
        InputType type = config.getType();

        return switch (type) {
            case CSV -> {
                String delimiter = config.getDelimiter() == null || config.getDelimiter().isEmpty() ? "," : config.getDelimiter();
                yield new CsvInputReader(path, delimiter.charAt(0), config.isHasHeader());
            }
            case JSONL -> new JsonlInputReader(path, objectMapper);
            case FIXED_WIDTH -> new FixedWidthInputReader(path, config.getFixedWidthFields());
        };
    }
}
