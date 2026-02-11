package com.fanout.engine.transform;

import com.fanout.engine.model.SourceRecord;
import com.fanout.engine.model.WideColumnPayload;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;

import java.io.ByteArrayOutputStream;
import java.util.LinkedHashMap;
import java.util.Map;

public final class WideColumnTransformer implements Transformer<WideColumnPayload> {
    private static final Schema SCHEMA = new Schema.Parser().parse("""
            {
              "type": "record",
              "name": "WideColumnRecord",
              "namespace": "com.fanout.engine.avro",
              "fields": [
                {"name": "sequence", "type": "long"},
                {"name": "fields", "type": {"type": "map", "values": "string"}}
              ]
            }
            """);

    @Override
    public WideColumnPayload transform(SourceRecord record) throws Exception {
        Map<String, Object> cqlMap = new LinkedHashMap<>(record.fields());
        Map<String, String> avroFields = new LinkedHashMap<>();
        for (Map.Entry<String, Object> entry : record.fields().entrySet()) {
            avroFields.put(entry.getKey(), String.valueOf(entry.getValue()));
        }

        GenericRecord avroRecord = new GenericData.Record(SCHEMA);
        avroRecord.put("sequence", record.sequence());
        avroRecord.put("fields", avroFields);

        byte[] avroBytes;
        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
            BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);
            new GenericDatumWriter<GenericRecord>(SCHEMA).write(avroRecord, encoder);
            encoder.flush();
            avroBytes = outputStream.toByteArray();
        }

        return new WideColumnPayload(avroBytes, cqlMap);
    }

    public static Schema schema() {
        return SCHEMA;
    }
}
