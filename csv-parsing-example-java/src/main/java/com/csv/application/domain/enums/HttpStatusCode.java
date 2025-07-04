package com.csv.application.domain.enums;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public enum HttpStatusCode {
    BAD_REQUEST(400),
    INTERNAL_SERVER_ERROR(500);

    private final int code;
}
