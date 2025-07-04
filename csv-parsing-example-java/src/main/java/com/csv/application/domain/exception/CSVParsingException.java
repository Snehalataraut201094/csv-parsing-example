package com.csv.application.domain.exception;

public class CSVParsingException extends RuntimeException {

    private String message;

    public CSVParsingException(String message) {
        super(message);
        this.message = message;
    }
}

