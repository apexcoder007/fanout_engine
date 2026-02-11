package com.fanout.engine.resilience;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fanout.engine.model.SourceRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DeadLetterQueueTest {

    @TempDir
    Path tempDir;

    @Test
    void shouldCreateParentDirectoriesAndWriteJsonlEntries() throws Exception {
        Path dlqPath = tempDir.resolve("deep/path/dead-letter.jsonl");
        Map<String, Object> fields = new LinkedHashMap<>();
        fields.put("id", 123);
        fields.put("name", "alice");
        SourceRecord record = new SourceRecord(9L, fields, "line");

        try (DeadLetterQueue deadLetterQueue = new DeadLetterQueue(dlqPath)) {
            deadLetterQueue.writeFailure(record, "rest", 3, new RuntimeException("boom"));
        }

        assertTrue(Files.exists(dlqPath));
        String line = Files.readAllLines(dlqPath).getFirst();
        JsonNode node = new ObjectMapper().readTree(line);

        assertEquals(9L, node.get("recordSequence").asLong());
        assertEquals("rest", node.get("sinkName").asText());
        assertEquals(3, node.get("attempts").asInt());
        assertEquals("boom", node.get("error").asText());
        assertEquals("alice", node.get("fields").get("name").asText());
    }
}
