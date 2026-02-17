package com.freightfox.jsondataset.dto.response;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.*;

import java.time.OffsetDateTime;
import java.util.UUID;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@ToString
@JsonInclude(JsonInclude.Include.NON_NULL)
public class InsertRecordResponse {
    @JsonProperty("success")
    @Builder.Default
    private Boolean success = true;

    @JsonProperty("message")
    private String message;

    @JsonProperty("record_id")
    private UUID recordId;

    @JsonProperty("dataset_name")
    private String datasetName;

    @JsonProperty("created_at")
    private OffsetDateTime createdAt;

    public static InsertRecordResponse success(UUID recordId, String datasetName, OffsetDateTime createdAt) {
        return InsertRecordResponse.builder()
                .success(true)
                .message("Record inserted successfully")
                .recordId(recordId)
                .datasetName(datasetName)
                .createdAt(createdAt)
                .build();
    }

    public static InsertRecordResponse error(String message) {
        return InsertRecordResponse.builder()
                .success(false)
                .message(message)
                .build();
    }
}
