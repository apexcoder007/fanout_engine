package com.fanout.engine.transform;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fanout.engine.model.SourceRecord;

import java.util.LinkedHashMap;
import java.util.Map;

public final class JsonTransformer implements Transformer<String> {
    private final ObjectMapper objectMapper;

    public JsonTransformer() {
        this.objectMapper = new ObjectMapper().findAndRegisterModules();
    }

    @Override
    public String transform(SourceRecord record) throws Exception {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("sequence", record.sequence());
        payload.putAll(record.fields());
        return objectMapper.writeValueAsString(payload);
    }
}
