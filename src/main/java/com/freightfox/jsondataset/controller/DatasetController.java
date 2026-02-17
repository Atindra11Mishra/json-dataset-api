package com.freightfox.jsondataset.controller;

import com.freightfox.jsondataset.dto.request.InsertRecordRequest;
import com.freightfox.jsondataset.dto.request.QueryRequest;
import com.freightfox.jsondataset.dto.response.InsertRecordResponse;
import com.freightfox.jsondataset.dto.response.QueryResponse;
import com.freightfox.jsondataset.exception.InvalidJsonException;
import com.freightfox.jsondataset.service.DatasetService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/datasets")
@RequiredArgsConstructor
@Slf4j
public class DatasetController {
    private final DatasetService datasetService;

    @PostMapping("/{datasetName}/record")
    public ResponseEntity<InsertRecordResponse> insertRecord(
            @PathVariable("datasetName") String datasetName,
            @Valid @RequestBody InsertRecordRequest request) {
        log.info("POST /api/datasets/{}/record - Inserting record", datasetName);
        log.debug("Request body: {}", request);

        if (!datasetName.equals(request.getDatasetName())) {
            log.warn("Dataset name mismatch. Path: {}, Body: {}",
                    datasetName, request.getDatasetName());
            request.setDatasetName(datasetName);
        }

        InsertRecordResponse response = datasetService.insertRecord(request);

        log.info("Record inserted successfully. ID: {}", response.getRecordId());

        return ResponseEntity
                .status(HttpStatus.CREATED)
                .body(response);
    }

    @GetMapping("/{datasetName}/query")
    public ResponseEntity<QueryResponse> query(
            @PathVariable("datasetName") String datasetName,
            @RequestParam(value = "groupBy", required = false) String groupBy,
            @RequestParam(value = "sortBy", required = false) String sortBy,
            @RequestParam(value = "order", required = false, defaultValue = "asc") String order) {
        log.info("GET /api/datasets/{}/query - groupBy: {}, sortBy: {}, order: {}",
                datasetName, groupBy, sortBy, order);

        if (groupBy == null && sortBy == null) {
            log.warn("Neither groupBy nor sortBy provided");
            throw new InvalidJsonException(
                    "At least one query parameter (groupBy or sortBy) must be provided");
        }

        QueryRequest request = QueryRequest.builder()
                .datasetName(datasetName)
                .groupBy(groupBy)
                .sortBy(sortBy)
                .sortOrder(order)
                .build();

        QueryResponse response;

        if (groupBy != null && sortBy != null) {
            log.info("Executing group-by-then-sort operation");
            response = datasetService.groupByThenSort(request);

        } else if (groupBy != null) {
            log.info("Executing group-by operation");
            response = datasetService.groupBy(request);

        } else {
            log.info("Executing sort-by operation");
            response = datasetService.sortBy(request);
        }

        log.info("Query completed. Operation: {}, Records: {}",
                response.getOperation(), response.getTotalRecords());

        return ResponseEntity.ok(response);
    }

    @GetMapping("/health")
    public ResponseEntity<HealthResponse> health() {
        log.debug("GET /api/datasets/health - Health check");

        HealthResponse response = HealthResponse.builder()
                .status("UP")
                .message("Dataset API is running")
                .build();

        return ResponseEntity.ok(response);
    }

    @lombok.Data
    @lombok.Builder
    @lombok.NoArgsConstructor
    @lombok.AllArgsConstructor
    private static class HealthResponse {
        private String status;
        private String message;
    }
}
