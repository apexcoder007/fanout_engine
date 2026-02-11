package com.fanout.engine.ingest;

import com.fanout.engine.config.FixedWidthFieldConfig;
import com.fanout.engine.model.SourceRecord;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

public final class FixedWidthInputReader implements InputReader {
    private final Path path;
    private final List<FixedWidthFieldConfig> fields;

    public FixedWidthInputReader(Path path, List<FixedWidthFieldConfig> fields) {
        this.path = path;
        this.fields = fields;
    }

    @Override
    public void stream(Consumer<SourceRecord> consumer) throws IOException {
        if (fields == null || fields.isEmpty()) {
            throw new IllegalArgumentException("Fixed-width input requires input.fixedWidthFields");
        }

        try (BufferedReader reader = Files.newBufferedReader(path)) {
            String line;
            long sequence = 0;
            while ((line = reader.readLine()) != null) {
                if (line.isBlank()) {
                    continue;
                }
                sequence++;
                Map<String, Object> parsed = new LinkedHashMap<>();
                for (FixedWidthFieldConfig field : fields) {
                    int start = Math.max(0, field.getStart());
                    int end = Math.min(line.length(), Math.max(start, field.getEnd()));
                    String slice = start >= line.length() ? "" : line.substring(start, end);
                    parsed.put(field.getName(), RecordParserUtil.parseScalar(slice));
                }
                consumer.accept(new SourceRecord(sequence, parsed, line));
            }
        }
    }
}
