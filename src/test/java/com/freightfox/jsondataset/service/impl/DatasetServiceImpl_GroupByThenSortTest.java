package com.freightfox.jsondataset.service.impl;

import com.freightfox.jsondataset.dto.request.QueryRequest;
import com.freightfox.jsondataset.dto.response.QueryResponse;
import com.freightfox.jsondataset.exception.InvalidFieldException;
import com.freightfox.jsondataset.model.entity.DatasetRecord;
import com.freightfox.jsondataset.repository.DatasetRepository;
import com.freightfox.jsondataset.util.JsonValidator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class DatasetServiceImpl_GroupByThenSortTest {

    @Mock
    private DatasetRepository datasetRepository;

    @Mock
    private JsonValidator jsonValidator;

    @InjectMocks
    private DatasetServiceImpl datasetService;

    private List<DatasetRecord> sampleRecords;

    @BeforeEach
    void setUp() {
        sampleRecords = new ArrayList<>();
    }

    @Test
    @DisplayName("Should group by status and sort by age ascending")
    void testGroupByThenSort_Success() {
        // Given
        // Active group
        sampleRecords.add(createRecord("active", 30)); // Middle
        sampleRecords.add(createRecord("active", 10)); // Youngest
        sampleRecords.add(createRecord("active", 50)); // Oldest

        // Inactive group
        sampleRecords.add(createRecord("inactive", 40));
        sampleRecords.add(createRecord("inactive", 20));

        QueryRequest request = QueryRequest.builder()
                .datasetName("users")
                .groupBy("status")
                .sortBy("age")
                .sortOrder("asc")
                .build();

        doNothing().when(jsonValidator).validateDatasetName(anyString());
        when(datasetRepository.findByDatasetNameAndIsDeleted(anyString(), eq(false)))
                .thenReturn(sampleRecords);

        // When
        QueryResponse response = datasetService.groupByThenSort(request);

        // Then
        assertThat(response.getSuccess()).isTrue();
        assertThat(response.getOperation()).isEqualTo("group_by_then_sort");
        assertThat(response.getGroups()).hasSize(2);

        // Check active group sorting
        List<Map<String, Object>> activeGroup = response.getGroups().get("active");
        assertThat(activeGroup).hasSize(3);
        assertThat(activeGroup).extracting(map -> map.get("age"))
                .containsExactly(10, 30, 50);

        // Check inactive group sorting
        List<Map<String, Object>> inactiveGroup = response.getGroups().get("inactive");
        assertThat(inactiveGroup).hasSize(2);
        assertThat(inactiveGroup).extracting(map -> map.get("age"))
                .containsExactly(20, 40);

        // Check metadata
        assertThat(response.getSortField()).isEqualTo("age");
    }

    @Test
    @DisplayName("Should group by status and sort by age descending")
    void testGroupByThenSort_Desc() {
        // Given
        sampleRecords.add(createRecord("active", 30));
        sampleRecords.add(createRecord("active", 10));

        QueryRequest request = QueryRequest.builder()
                .datasetName("users")
                .groupBy("status")
                .sortBy("age")
                .sortOrder("desc")
                .build();

        doNothing().when(jsonValidator).validateDatasetName(anyString());
        when(datasetRepository.findByDatasetNameAndIsDeleted(anyString(), eq(false)))
                .thenReturn(sampleRecords);

        // When
        QueryResponse response = datasetService.groupByThenSort(request);

        // Then
        List<Map<String, Object>> activeGroup = response.getGroups().get("active");
        assertThat(activeGroup).extracting(map -> map.get("age"))
                .containsExactly(30, 10);
    }

    @Test
    @DisplayName("Should throw exception if sortBy is missing")
    void testGroupByThenSort_MissingSortBy() {
        QueryRequest request = QueryRequest.builder()
                .datasetName("users")
                .groupBy("status")
                // No sortBy
                .sortOrder("asc")
                .build();

        // Validation should fail before mocking
        assertThatThrownBy(() -> datasetService.groupByThenSort(request))
                .isInstanceOf(InvalidFieldException.class)
                .hasMessageContaining("Sort-by field is required");
    }

    private DatasetRecord createRecord(String status, int age) {
        Map<String, Object> data = new HashMap<>();
        data.put("status", status);
        data.put("age", age);
        data.put("name", "User" + age);

        return DatasetRecord.builder()
                .datasetName("users")
                .data(data)
                .isDeleted(false)
                .build();
    }
}
