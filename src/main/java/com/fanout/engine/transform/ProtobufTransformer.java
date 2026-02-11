package com.fanout.engine.transform;

import com.fanout.engine.model.SourceRecord;
import com.google.protobuf.ListValue;
import com.google.protobuf.NullValue;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;

import java.util.Map;

public final class ProtobufTransformer implements Transformer<byte[]> {
    @Override
    public byte[] transform(SourceRecord record) {
        Struct.Builder structBuilder = Struct.newBuilder();
        structBuilder.putFields("sequence", Value.newBuilder().setNumberValue(record.sequence()).build());

        for (Map.Entry<String, Object> entry : record.fields().entrySet()) {
            structBuilder.putFields(entry.getKey(), toValue(entry.getValue()));
        }

        return structBuilder.build().toByteArray();
    }

    private Value toValue(Object input) {
        if (input == null) {
            return Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build();
        }
        if (input instanceof Boolean boolValue) {
            return Value.newBuilder().setBoolValue(boolValue).build();
        }
        if (input instanceof Number number) {
            return Value.newBuilder().setNumberValue(number.doubleValue()).build();
        }
        if (input instanceof Map<?, ?> map) {
            Struct.Builder struct = Struct.newBuilder();
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                struct.putFields(String.valueOf(entry.getKey()), toValue(entry.getValue()));
            }
            return Value.newBuilder().setStructValue(struct.build()).build();
        }
        if (input instanceof Iterable<?> iterable) {
            ListValue.Builder listValue = ListValue.newBuilder();
            for (Object item : iterable) {
                listValue.addValues(toValue(item));
            }
            return Value.newBuilder().setListValue(listValue.build()).build();
        }
        return Value.newBuilder().setStringValue(String.valueOf(input)).build();
    }
}
