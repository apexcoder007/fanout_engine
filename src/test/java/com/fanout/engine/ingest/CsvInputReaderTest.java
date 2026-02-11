package com.fanout.engine.ingest;

import com.fanout.engine.model.SourceRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

class CsvInputReaderTest {

    @TempDir
    Path tempDir;

    @Test
    void shouldStreamCsvWithHeaderAndInferScalarTypes() throws Exception {
        Path csv = tempDir.resolve("records.csv");
        Files.writeString(csv, "id,active,balance,name\n1,true,10.5,Alice\n2,false,0.0,Bob\n");

        CsvInputReader reader = new CsvInputReader(csv, ',', true);
        List<SourceRecord> records = new ArrayList<>();
        reader.stream(records::add);

        assertEquals(2, records.size());
        assertEquals(1L, records.get(0).sequence());
        assertEquals(2L, records.get(1).sequence());
        assertInstanceOf(Long.class, records.get(0).fields().get("id"));
        assertInstanceOf(Boolean.class, records.get(0).fields().get("active"));
        assertInstanceOf(Double.class, records.get(0).fields().get("balance"));
        assertEquals("Alice", records.get(0).fields().get("name"));
    }

    @Test
    void shouldStreamCsvWithoutHeaderUsingColumnIndexes() throws Exception {
        Path csv = tempDir.resolve("records.csv");
        Files.writeString(csv, "10|foo\n20|bar\n");

        CsvInputReader reader = new CsvInputReader(csv, '|', false);
        List<SourceRecord> records = new ArrayList<>();
        reader.stream(records::add);

        assertEquals(2, records.size());
        assertEquals(10L, records.get(0).fields().get("column_0"));
        assertEquals("foo", records.get(0).fields().get("column_1"));
        assertEquals(20L, records.get(1).fields().get("column_0"));
        assertEquals("bar", records.get(1).fields().get("column_1"));
    }
}
