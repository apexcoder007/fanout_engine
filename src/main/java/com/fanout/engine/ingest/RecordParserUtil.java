package com.fanout.engine.ingest;

final class RecordParserUtil {
    private RecordParserUtil() {
    }

    static Object parseScalar(String raw) {
        if (raw == null) {
            return null;
        }

        String value = raw.trim();
        if (value.isEmpty()) {
            return "";
        }

        if ("true".equalsIgnoreCase(value) || "false".equalsIgnoreCase(value)) {
            return Boolean.parseBoolean(value);
        }

        try {
            if (value.contains(".")) {
                return Double.parseDouble(value);
            }
            return Long.parseLong(value);
        } catch (NumberFormatException ignored) {
            return value;
        }
    }
}
