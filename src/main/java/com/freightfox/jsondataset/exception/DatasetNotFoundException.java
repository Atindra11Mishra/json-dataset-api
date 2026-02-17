package com.freightfox.jsondataset.exception;

import lombok.Getter;

@Getter
public class DatasetNotFoundException extends RuntimeException {

    private final String errorCode;
    private final String datasetName;

    public DatasetNotFoundException(String message) {
        super(message);
        this.errorCode = "DATASET_NOT_FOUND";
        this.datasetName = null;
    }

    public DatasetNotFoundException(String datasetName, String message) {
        super(message);
        this.errorCode = "DATASET_NOT_FOUND";
        this.datasetName = datasetName;
    }
}
