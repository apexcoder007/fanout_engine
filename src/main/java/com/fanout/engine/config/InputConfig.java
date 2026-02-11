package com.fanout.engine.config;

import java.util.ArrayList;
import java.util.List;

public class InputConfig {
    private InputType type = InputType.CSV;
    private String path;
    private String delimiter = ",";
    private boolean hasHeader = true;
    private List<FixedWidthFieldConfig> fixedWidthFields = new ArrayList<>();

    public InputType getType() {
        return type;
    }

    public void setType(InputType type) {
        this.type = type;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public String getDelimiter() {
        return delimiter;
    }

    public void setDelimiter(String delimiter) {
        this.delimiter = delimiter;
    }

    public boolean isHasHeader() {
        return hasHeader;
    }

    public void setHasHeader(boolean hasHeader) {
        this.hasHeader = hasHeader;
    }

    public List<FixedWidthFieldConfig> getFixedWidthFields() {
        return fixedWidthFields;
    }

    public void setFixedWidthFields(List<FixedWidthFieldConfig> fixedWidthFields) {
        this.fixedWidthFields = fixedWidthFields;
    }
}
