package com.fanout.engine.transform;

import com.fanout.engine.model.SourceRecord;

import java.util.Map;

public final class XmlTransformer implements Transformer<String> {
    @Override
    public String transform(SourceRecord record) {
        StringBuilder sb = new StringBuilder();
        sb.append("<record sequence=\"").append(record.sequence()).append("\">");
        for (Map.Entry<String, Object> entry : record.fields().entrySet()) {
            sb.append("<").append(safeTagName(entry.getKey())).append(">")
                    .append(escapeXml(String.valueOf(entry.getValue())))
                    .append("</").append(safeTagName(entry.getKey())).append(">");
        }
        sb.append("</record>");
        return sb.toString();
    }

    private String safeTagName(String input) {
        String cleaned = input.replaceAll("[^A-Za-z0-9_\\-]", "_");
        if (cleaned.isEmpty() || Character.isDigit(cleaned.charAt(0))) {
            return "f_" + cleaned;
        }
        return cleaned;
    }

    private String escapeXml(String value) {
        return value
                .replace("&", "&amp;")
                .replace("<", "&lt;")
                .replace(">", "&gt;")
                .replace("\"", "&quot;")
                .replace("'", "&apos;");
    }
}
