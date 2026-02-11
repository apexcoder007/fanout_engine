package com.fanout.engine.ingest;

import com.fanout.engine.model.SourceRecord;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

public final class CsvInputReader implements InputReader {
    private final Path path;
    private final char delimiter;
    private final boolean hasHeader;

    public CsvInputReader(Path path, char delimiter, boolean hasHeader) {
        this.path = path;
        this.delimiter = delimiter;
        this.hasHeader = hasHeader;
    }

    @Override
    public void stream(Consumer<SourceRecord> consumer) throws IOException {
        CSVFormat.Builder formatBuilder = CSVFormat.DEFAULT.builder()
                .setDelimiter(delimiter)
                .setIgnoreSurroundingSpaces(true)
                .setTrim(true);

        if (hasHeader) {
            formatBuilder.setHeader().setSkipHeaderRecord(true);
        }

        CSVFormat format = formatBuilder.get();

        try (Reader reader = Files.newBufferedReader(path); CSVParser parser = format.parse(reader)) {
            long sequence = 0;
            List<String> headers = hasHeader ? parser.getHeaderNames() : List.of();
            for (CSVRecord csvRecord : parser) {
                sequence++;
                Map<String, Object> fields = new LinkedHashMap<>();
                if (hasHeader && !headers.isEmpty()) {
                    for (String header : headers) {
                        fields.put(header, RecordParserUtil.parseScalar(csvRecord.get(header)));
                    }
                } else {
                    for (int i = 0; i < csvRecord.size(); i++) {
                        fields.put("column_" + i, RecordParserUtil.parseScalar(csvRecord.get(i)));
                    }
                }
                consumer.accept(new SourceRecord(sequence, fields, csvRecord.toString()));
            }
        }
    }
}
