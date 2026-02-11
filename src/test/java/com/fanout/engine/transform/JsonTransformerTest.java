package com.fanout.engine.transform;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fanout.engine.model.SourceRecord;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class JsonTransformerTest {
    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    void shouldTransformToJsonPayload() throws Exception {
        JsonTransformer transformer = new JsonTransformer();
        Map<String, Object> fields = new LinkedHashMap<>();
        fields.put("id", 10);
        fields.put("name", "alice");

        String payload = transformer.transform(new SourceRecord(7, fields, ""));
        Map<String, Object> decoded = mapper.readValue(payload, new TypeReference<Map<String, Object>>() {
        });

        assertEquals(7, ((Number) decoded.get("sequence")).intValue());
        assertEquals(10, ((Number) decoded.get("id")).intValue());
        assertEquals("alice", decoded.get("name"));
    }
}
