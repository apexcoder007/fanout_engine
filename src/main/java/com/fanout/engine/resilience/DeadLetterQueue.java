package com.fanout.engine.resilience;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fanout.engine.model.DeadLetterEntry;
import com.fanout.engine.model.SourceRecord;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;

public final class DeadLetterQueue implements AutoCloseable {
    private final ObjectMapper objectMapper;
    private final BufferedWriter writer;

    public DeadLetterQueue(Path path) throws IOException {
        this.objectMapper = new ObjectMapper().findAndRegisterModules();
        Path parent = path.getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        this.writer = Files.newBufferedWriter(path,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.WRITE);
    }

    public synchronized void writeFailure(SourceRecord record,
                                          String sinkName,
                                          int attempts,
                                          Throwable error) {
        DeadLetterEntry entry = new DeadLetterEntry(
                Instant.now(),
                record.sequence(),
                sinkName,
                attempts,
                error == null ? "unknown" : String.valueOf(error.getMessage()),
                record.fields()
        );

        try {
            writer.write(objectMapper.writeValueAsString(entry));
            writer.newLine();
            writer.flush();
        } catch (IOException ioException) {
            throw new RuntimeException("Failed to write dead-letter entry", ioException);
        }
    }

    @Override
    public void close() throws IOException {
        writer.close();
    }
}
