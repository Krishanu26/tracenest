package com.dataflow.engine;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.springframework.stereotype.Component;
import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Component
public class CsvDatasetReader {
    public Dataset read(Path path) throws IOException {
        try (Reader reader = Files.newBufferedReader(path, StandardCharsets.UTF_8);
             CSVParser parser = CSVFormat.DEFAULT.builder().setHeader().setSkipHeaderRecord(true).setTrim(true).get().parse(reader)) {
            List<Map<String, String>> rows = new ArrayList<>();
            for (CSVRecord record : parser) {
                Map<String, String> row = new java.util.LinkedHashMap<>();
                for (String header : parser.getHeaderNames()) row.put(header, record.get(header));
                rows.add(Map.copyOf(row));
            }
            return new Dataset(parser.getHeaderNames(), rows);
        }
    }
    public record Dataset(List<String> headers, List<Map<String, String>> rows) {}
}
