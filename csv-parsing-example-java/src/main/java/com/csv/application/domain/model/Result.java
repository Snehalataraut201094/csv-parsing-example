package com.csv.application.domain.model;

public record Result<T>(T data, ErrorResponse error) {

    public static <T> Result<T> success(T data) {
        return new Result<>(data, null);
    }

    public static <T> Result<T> failure(ErrorResponse error) {
        return new Result<>(null, error);
    }

    public boolean isSuccess() { return error == null; }
}

