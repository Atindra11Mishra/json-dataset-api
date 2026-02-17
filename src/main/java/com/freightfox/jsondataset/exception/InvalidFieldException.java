package com.freightfox.jsondataset.exception;

import lombok.Getter;

@Getter
public class InvalidFieldException extends RuntimeException {

    private final String errorCode;
    private final String fieldName;
    private final String datasetName;

    public InvalidFieldException(String fieldName, String datasetName) {
        super(String.format("Invalid or missing field '%s' in dataset '%s'",
                fieldName, datasetName));
        this.errorCode = "INVALID_FIELD";
        this.fieldName = fieldName;
        this.datasetName = datasetName;
    }

    public InvalidFieldException(String fieldName, String datasetName, String message) {
        super(message);
        this.errorCode = "INVALID_FIELD";
        this.fieldName = fieldName;
        this.datasetName = datasetName;
    }

    public InvalidFieldException(String fieldName, String datasetName, String message, Throwable cause) {
        super(message, cause);
        this.errorCode = "INVALID_FIELD";
        this.fieldName = fieldName;
        this.datasetName = datasetName;
    }
}
