package com.fanout.engine.ingest;

import com.fanout.engine.config.FixedWidthFieldConfig;
import com.fanout.engine.config.InputConfig;
import com.fanout.engine.config.InputType;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;

class InputReaderFactoryTest {

    @Test
    void shouldCreateCsvReader() {
        InputConfig config = new InputConfig();
        config.setType(InputType.CSV);
        config.setPath("samples/customers.csv");
        config.setDelimiter(",");

        InputReader reader = new InputReaderFactory().create(config);

        assertInstanceOf(CsvInputReader.class, reader);
    }

    @Test
    void shouldCreateCsvReaderWithDefaultDelimiterWhenEmpty() {
        InputConfig config = new InputConfig();
        config.setType(InputType.CSV);
        config.setPath("samples/customers.csv");
        config.setDelimiter("");

        InputReader reader = new InputReaderFactory().create(config);

        assertInstanceOf(CsvInputReader.class, reader);
    }

    @Test
    void shouldCreateJsonlReader() {
        InputConfig config = new InputConfig();
        config.setType(InputType.JSONL);
        config.setPath("samples/customers.jsonl");

        InputReader reader = new InputReaderFactory().create(config);

        assertInstanceOf(JsonlInputReader.class, reader);
    }

    @Test
    void shouldCreateFixedWidthReader() {
        InputConfig config = new InputConfig();
        config.setType(InputType.FIXED_WIDTH);
        config.setPath("samples/customers_fixed_width.txt");
        FixedWidthFieldConfig field = new FixedWidthFieldConfig();
        field.setName("id");
        field.setStart(0);
        field.setEnd(6);
        config.setFixedWidthFields(List.of(field));

        InputReader reader = new InputReaderFactory().create(config);

        assertInstanceOf(FixedWidthInputReader.class, reader);
    }
}
