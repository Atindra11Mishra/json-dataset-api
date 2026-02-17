package com.freightfox.jsondataset.dto.request;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.*;

import java.util.Map;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@ToString
public class InsertRecordRequest {

    
    @NotBlank(message = "Dataset name is required and cannot be blank")
    @JsonProperty("dataset_name")
    private String datasetName;

    
    @NotNull(message = "Data is required and cannot be null")
    @JsonProperty("data")
    private Map<String, Object> data;

    
    public boolean isValid() {
        if (datasetName == null || datasetName.trim().isEmpty()) {
            return false;
        }
        if (data == null) {
            return false;
        }
        return true;
    }
}