package com.fanout.engine.transform;

import com.fanout.engine.model.SourceRecord;
import com.google.protobuf.Struct;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ProtobufTransformerTest {
    @Test
    void shouldTransformToProtobufStructBytes() throws Exception {
        ProtobufTransformer transformer = new ProtobufTransformer();
        Map<String, Object> fields = new LinkedHashMap<>();
        fields.put("id", 12);
        fields.put("name", "bob");

        byte[] payload = transformer.transform(new SourceRecord(3, fields, ""));
        Struct struct = Struct.parseFrom(payload);

        assertEquals(3.0, struct.getFieldsOrThrow("sequence").getNumberValue());
        assertEquals(12.0, struct.getFieldsOrThrow("id").getNumberValue());
        assertEquals("bob", struct.getFieldsOrThrow("name").getStringValue());
    }
}
