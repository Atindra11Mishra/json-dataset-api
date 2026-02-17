package com.freightfox.jsondataset.exception;

import lombok.Getter;

@Getter
public class InvalidSortOrderException extends RuntimeException {

    private final String errorCode;
    private final String providedOrder;
    private final String datasetName;

    public InvalidSortOrderException(String providedOrder, String datasetName) {
        super(String.format(
                "Invalid sort order '%s' for dataset '%s'. Must be 'asc' or 'desc'", 
                providedOrder, datasetName));
        this.errorCode = "INVALID_SORT_ORDER";
        this.providedOrder = providedOrder;
        this.datasetName = datasetName;
    }

    public InvalidSortOrderException(String providedOrder, String datasetName, String message) {
        super(message);
        this.errorCode = "INVALID_SORT_ORDER";
        this.providedOrder = providedOrder;
        this.datasetName = datasetName;
    }
}