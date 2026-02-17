package com.freightfox.jsondataset.service;

import com.freightfox.jsondataset.dto.request.InsertRecordRequest;
import com.freightfox.jsondataset.dto.request.QueryRequest;
import com.freightfox.jsondataset.dto.response.InsertRecordResponse;
import com.freightfox.jsondataset.dto.response.QueryResponse;

public interface DatasetService {

    InsertRecordResponse insertRecord(InsertRecordRequest request);

    QueryResponse groupBy(QueryRequest request);

    QueryResponse sortBy(QueryRequest request);

    
    QueryResponse groupByThenSort(QueryRequest request);
}