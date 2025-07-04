package com.csv.application.domain.model;

import java.util.Map;

public record Row(Map<String, String> rows){

    public String get(String columnName) {
        return rows.get(columnName);
    }

    public void set(String columnName, String value) {
        rows.put(columnName, value);
    }

    public Map<String, String> getData() {
        return rows;
    }
}
