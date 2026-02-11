package com.fanout.engine.ingest;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fanout.engine.model.SourceRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class JsonlInputReaderTest {

    @TempDir
    Path tempDir;

    @Test
    void shouldSkipBlankLinesAndStreamJsonlRecords() throws Exception {
        Path jsonl = tempDir.resolve("records.jsonl");
        Files.writeString(jsonl, """
                {"id":1,"name":"Alice"}

                {"id":2,"name":"Bob","nested":{"x":7}}
                """);

        JsonlInputReader reader = new JsonlInputReader(jsonl, new ObjectMapper().findAndRegisterModules());
        List<SourceRecord> records = new ArrayList<>();
        reader.stream(records::add);

        assertEquals(2, records.size());
        assertEquals(1L, records.get(0).sequence());
        assertEquals(2L, records.get(1).sequence());
        assertEquals(1, ((Number) records.get(0).fields().get("id")).intValue());
        assertEquals("Bob", records.get(1).fields().get("name"));
        Map<?, ?> nested = (Map<?, ?>) records.get(1).fields().get("nested");
        assertEquals(7, ((Number) nested.get("x")).intValue());
    }

    @Test
    void shouldThrowOnMalformedJsonLine() throws Exception {
        Path jsonl = tempDir.resolve("records.jsonl");
        Files.writeString(jsonl, "{" + "\"id\":1" + "\n" + "not-json\n");

        JsonlInputReader reader = new JsonlInputReader(jsonl, new ObjectMapper().findAndRegisterModules());
        assertThrows(IOException.class, () -> reader.stream(ignored -> {
        }));
    }
}
