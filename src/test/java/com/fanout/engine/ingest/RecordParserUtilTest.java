package com.fanout.engine.ingest;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RecordParserUtilTest {

    @Test
    void shouldParseNullAndEmpty() {
        assertNull(RecordParserUtil.parseScalar(null));
        assertEquals("", RecordParserUtil.parseScalar("   "));
    }

    @Test
    void shouldParseBooleansNumbersAndStrings() {
        assertEquals(true, RecordParserUtil.parseScalar("true"));
        assertEquals(false, RecordParserUtil.parseScalar("FALSE"));
        assertEquals(42L, RecordParserUtil.parseScalar("42"));
        assertEquals(3.14d, RecordParserUtil.parseScalar("3.14"));
        Object parsed = RecordParserUtil.parseScalar("abc123");
        assertTrue(parsed instanceof String);
        assertEquals("abc123", parsed);
    }
}
