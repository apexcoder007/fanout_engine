package com.fanout.engine.transform;

import com.fanout.engine.model.SourceRecord;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertTrue;

class XmlTransformerTest {
    @Test
    void shouldTransformToXmlPayload() {
        XmlTransformer transformer = new XmlTransformer();
        Map<String, Object> fields = new LinkedHashMap<>();
        fields.put("first_name", "alice");
        fields.put("note", "A&B");

        String payload = transformer.transform(new SourceRecord(1, fields, ""));

        assertTrue(payload.contains("<record sequence=\"1\">"));
        assertTrue(payload.contains("<first_name>alice</first_name>"));
        assertTrue(payload.contains("<note>A&amp;B</note>"));
    }
}
