package com.fanout.engine.transform;

import com.fanout.engine.model.SourceRecord;
import com.fanout.engine.model.WideColumnPayload;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class WideColumnTransformerTest {
    @Test
    void shouldTransformToAvroAndCqlMap() throws Exception {
        WideColumnTransformer transformer = new WideColumnTransformer();
        Map<String, Object> fields = new LinkedHashMap<>();
        fields.put("id", 99);
        fields.put("name", "charlie");

        WideColumnPayload payload = transformer.transform(new SourceRecord(5, fields, ""));

        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(payload.avroBytes(), null);
        GenericRecord decoded = new GenericDatumReader<GenericRecord>(WideColumnTransformer.schema())
                .read(null, decoder);

        assertEquals(5L, decoded.get("sequence"));
        Map<?, ?> decodedMap = (Map<?, ?>) decoded.get("fields");
        assertEquals("99", mapValueByStringKey(decodedMap, "id"));
        assertEquals("charlie", mapValueByStringKey(decodedMap, "name"));

        assertEquals(99L, ((Number) payload.cqlMap().get("id")).longValue());
        assertEquals("charlie", payload.cqlMap().get("name"));
    }

    private String mapValueByStringKey(Map<?, ?> map, String expectedKey) {
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            if (expectedKey.equals(String.valueOf(entry.getKey()))) {
                return String.valueOf(entry.getValue());
            }
        }
        return null;
    }
}
