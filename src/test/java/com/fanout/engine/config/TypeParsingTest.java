package com.fanout.engine.config;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class TypeParsingTest {

    @Test
    void shouldParseInputTypeAliases() {
        assertEquals(InputType.CSV, InputType.fromValue("csv"));
        assertEquals(InputType.JSONL, InputType.fromValue("json_lines"));
        assertEquals(InputType.FIXED_WIDTH, InputType.fromValue("fixed-width"));
        assertEquals(InputType.CSV, InputType.fromValue(null));
    }

    @Test
    void shouldRejectUnsupportedInputType() {
        assertThrows(IllegalArgumentException.class, () -> InputType.fromValue("xml"));
    }

    @Test
    void shouldParseSinkTypeAliases() {
        assertEquals(SinkType.REST_API, SinkType.fromValue("http2"));
        assertEquals(SinkType.GRPC, SinkType.fromValue("grpc"));
        assertEquals(SinkType.MESSAGE_QUEUE, SinkType.fromValue("kafka"));
        assertEquals(SinkType.WIDE_COLUMN_DB, SinkType.fromValue("cassandra"));
    }

    @Test
    void shouldRejectInvalidSinkType() {
        assertThrows(IllegalArgumentException.class, () -> SinkType.fromValue(null));
        assertThrows(IllegalArgumentException.class, () -> SinkType.fromValue("unknown"));
    }
}
