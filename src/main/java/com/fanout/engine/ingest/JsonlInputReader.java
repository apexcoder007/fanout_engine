package com.fanout.engine.ingest;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fanout.engine.model.SourceRecord;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Consumer;

public final class JsonlInputReader implements InputReader {
    private final Path path;
    private final ObjectMapper mapper;

    public JsonlInputReader(Path path, ObjectMapper mapper) {
        this.path = path;
        this.mapper = mapper;
    }

    @Override
    public void stream(Consumer<SourceRecord> consumer) throws IOException {
        try (BufferedReader reader = Files.newBufferedReader(path)) {
            String line;
            long sequence = 0;
            while ((line = reader.readLine()) != null) {
                if (line.isBlank()) {
                    continue;
                }
                sequence++;
                Map<String, Object> fields = mapper.readValue(line, new TypeReference<LinkedHashMap<String, Object>>() {
                });
                consumer.accept(new SourceRecord(sequence, fields, line));
            }
        }
    }
}
