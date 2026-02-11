package com.fanout.engine.ingest;

import com.fanout.engine.config.FixedWidthFieldConfig;
import com.fanout.engine.model.SourceRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class FixedWidthInputReaderTest {

    @TempDir
    Path tempDir;

    @Test
    void shouldParseConfiguredSlicesAndHandleOutOfBoundsRanges() throws Exception {
        Path fixed = tempDir.resolve("records.txt");
        Files.writeString(fixed, "000123Alice     US\n000124Bob       IN\n");

        FixedWidthInputReader reader = new FixedWidthInputReader(fixed, List.of(
                field("id", 0, 6),
                field("name", 6, 16),
                field("country", 16, 30),
                field("missing", 40, 50)
        ));

        List<SourceRecord> records = new ArrayList<>();
        reader.stream(records::add);

        assertEquals(2, records.size());
        assertEquals(123L, records.get(0).fields().get("id"));
        assertEquals("Alice", records.get(0).fields().get("name"));
        assertEquals("US", records.get(0).fields().get("country"));
        assertEquals("", records.get(0).fields().get("missing"));
    }

    @Test
    void shouldRequireFieldConfiguration() throws Exception {
        Path fixed = tempDir.resolve("records.txt");
        Files.writeString(fixed, "000123Alice     US\n");

        FixedWidthInputReader reader = new FixedWidthInputReader(fixed, List.of());
        assertThrows(IllegalArgumentException.class, () -> reader.stream(ignored -> {
        }));
    }

    private FixedWidthFieldConfig field(String name, int start, int end) {
        FixedWidthFieldConfig config = new FixedWidthFieldConfig();
        config.setName(name);
        config.setStart(start);
        config.setEnd(end);
        return config;
    }
}
