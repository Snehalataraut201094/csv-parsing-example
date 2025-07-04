package com.csv.application.domain.exception;

public class EmptyHeaderException extends RuntimeException {

    private String message;

    public EmptyHeaderException(String message) {
        super(message);
        this.message = message;
    }
}

