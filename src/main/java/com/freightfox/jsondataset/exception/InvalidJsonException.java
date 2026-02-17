package com.freightfox.jsondataset.exception;

import lombok.Getter;

import java.util.List;

@Getter
public class InvalidJsonException extends RuntimeException {
    private final String errorCode;
    private final List<String> validationErrors;

    public InvalidJsonException(String message) {
        super(message);
        this.errorCode = "INVALID_JSON";
        this.validationErrors = null;
    }

    public InvalidJsonException(String message, List<String> errors) {
        super(message);
        this.errorCode = "INVALID_JSON";
        this.validationErrors = errors;
    }

    public List<String> getErrors() {
        return validationErrors;
    }
}
