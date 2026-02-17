package com.freightfox.jsondataset.dto.request;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotBlank;
import lombok.*;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@ToString
public class QueryRequest {

    @NotBlank(message = "Dataset name is required and cannot be blank")
    @JsonProperty("dataset_name")
    private String datasetName;

    @JsonProperty("group_by")
    private String groupBy;

    @JsonProperty("sort_by")
    private String sortBy;

    
    @JsonProperty("sort_order")
    private String sortOrder;

    public boolean isValid() {
        if (datasetName == null || datasetName.trim().isEmpty()) {
            return false;
        }
        return groupBy != null || sortBy != null;
    }

    public boolean isGroupByQuery() {
        return groupBy != null && !groupBy.trim().isEmpty();
    }

    public boolean isSortByQuery() {
        return sortBy != null && !sortBy.trim().isEmpty();
    }
}