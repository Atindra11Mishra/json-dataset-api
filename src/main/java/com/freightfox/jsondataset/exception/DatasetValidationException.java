package com.freightfox.jsondataset.exception;

import lombok.Getter;

import java.util.List;

@Getter
public class DatasetValidationException extends RuntimeException {

    private final String errorCode;
    private final List<String> validationErrors;

    public DatasetValidationException(String message) {
        super(message);
        this.errorCode = "DATASET_VALIDATION_ERROR";
        this.validationErrors = null;
    }

    public DatasetValidationException(String message, List<String> validationErrors) {
        super(message);
        this.errorCode = "DATASET_VALIDATION_ERROR";
        this.validationErrors = validationErrors;
    }

    public DatasetValidationException(String message, List<String> validationErrors, Throwable cause) {
        super(message, cause);
        this.errorCode = "DATASET_VALIDATION_ERROR";
        this.validationErrors = validationErrors;
    }
}
