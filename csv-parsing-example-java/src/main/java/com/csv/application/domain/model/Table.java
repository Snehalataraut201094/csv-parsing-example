package com.csv.application.domain.model;

import java.util.List;

public record Table(List<String> headers, List<Row> rows) {

}