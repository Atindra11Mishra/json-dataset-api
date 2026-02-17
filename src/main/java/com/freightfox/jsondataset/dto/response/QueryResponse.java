package com.freightfox.jsondataset.dto.response;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.*;

import java.util.List;
import java.util.Map;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@ToString
@JsonInclude(JsonInclude.Include.NON_NULL)
public class QueryResponse {

    
    @JsonProperty("success")
    @Builder.Default
    private Boolean success = true;

    
    @JsonProperty("dataset_name")
    private String datasetName;

    
    @JsonProperty("operation")
    private String operation;

    
    @JsonProperty("field")
    private String field;

    
    @JsonProperty("sort_field")
    private String sortField;

    
    @JsonProperty("total_records")
    private Integer totalRecords;

    
    @JsonProperty("groups")
    private Map<String, List<Map<String, Object>>> groups;

    
    @JsonProperty("results")
    private List<Map<String, Object>> results;

    
    @JsonProperty("metadata")
    private QueryMetadata metadata;

    
    @JsonProperty("sort_metadata")
    private SortMetadata sortMetadata;

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    public static class QueryMetadata {

        
        @JsonProperty("total_groups")
        private Integer totalGroups;

        
        @JsonProperty("records_with_missing_field")
        private Integer recordsWithMissingField;

        
        @JsonProperty("records_with_null_field")
        private Integer recordsWithNullField;

        
        @JsonProperty("group_sizes")
        private Map<String, Integer> groupSizes;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    public static class SortMetadata {
        @JsonProperty("sort_order")
        private String sortOrder;

        @JsonProperty("field_type")
        private String fieldType;

        @JsonProperty("records_with_missing_field")
        private Integer recordsWithMissingField;

        @JsonProperty("records_with_null_field")
        private Integer recordsWithNullField;

        @JsonProperty("records_with_type_mismatch")
        private Integer recordsWithTypeMismatch;

        @JsonProperty("warnings")
        private List<String> warnings;
    }

    
    public static QueryResponse groupBySuccess(
            String datasetName,
            String field,
            String sortField,
            Map<String, List<Map<String, Object>>> groups,
            QueryMetadata metadata) {
        return QueryResponse.builder()
                .success(true)
                .datasetName(datasetName)
                .operation("group_by")
                .field(field)
                .sortField(sortField)
                .totalRecords(calculateTotalRecords(groups, metadata))
                .groups(groups)
                .metadata(metadata)
                .build();
    }

    public static QueryResponse sortBySuccess(
            String datasetName,
            String field,
            String sortOrder,
            List<Map<String, Object>> results,
            SortMetadata sortMetadata) {
        return QueryResponse.builder()
                .success(true)
                .datasetName(datasetName)
                .operation("sort_by")
                .field(field)
                .totalRecords(results.size())
                .results(results)
                .sortMetadata(sortMetadata)
                .build();
    }

    
    private static Integer calculateTotalRecords(
            Map<String, List<Map<String, Object>>> groups,
            QueryMetadata metadata) {
        return groups.values().stream()
                .mapToInt(List::size)
                .sum();
    }
}
