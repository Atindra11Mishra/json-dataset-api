package com.freightfox.jsondataset.service.impl;

import com.freightfox.jsondataset.dto.request.QueryRequest;
import com.freightfox.jsondataset.dto.response.QueryResponse;
import com.freightfox.jsondataset.exception.InvalidFieldException;
import com.freightfox.jsondataset.exception.InvalidJsonException;
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

import java.time.OffsetDateTime;
import java.util.*;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

/**
 * Unit tests for group-by functionality in DatasetServiceImpl.
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("DatasetService Group-By Tests")
class DatasetServiceImpl_GroupByTest {

    @Mock
    private DatasetRepository datasetRepository;

    @Mock
    private JsonValidator jsonValidator;

    @InjectMocks
    private DatasetServiceImpl datasetService;

    private QueryRequest validQueryRequest;
    private List<DatasetRecord> sampleRecords;

    @BeforeEach
    void setUp() {
        validQueryRequest = QueryRequest.builder()
                .datasetName("users")
                .groupBy("status")
                .build();

        // Setup sample records with different statuses
        sampleRecords = createSampleRecords();
    }

    private List<DatasetRecord> createSampleRecords() {
        List<DatasetRecord> records = new ArrayList<>();

        // Record 1: status = "active"
        Map<String, Object> data1 = new HashMap<>();
        data1.put("name", "John Doe");
        data1.put("age", 30);
        data1.put("status", "active");
        records.add(createRecord("users", data1));

        // Record 2: status = "active"
        Map<String, Object> data2 = new HashMap<>();
        data2.put("name", "Jane Smith");
        data2.put("age", 25);
        data2.put("status", "active");
        records.add(createRecord("users", data2));

        // Record 3: status = "inactive"
        Map<String, Object> data3 = new HashMap<>();
        data3.put("name", "Bob Johnson");
        data3.put("age", 45);
        data3.put("status", "inactive");
        records.add(createRecord("users", data3));

        // Record 4: status = "pending"
        Map<String, Object> data4 = new HashMap<>();
        data4.put("name", "Alice Brown");
        data4.put("age", 35);
        data4.put("status", "pending");
        records.add(createRecord("users", data4));

        return records;
    }

    private DatasetRecord createRecord(String datasetName, Map<String, Object> data) {
        return DatasetRecord.builder()
                .id(UUID.randomUUID())
                .datasetName(datasetName)
                .data(data)
                .createdAt(OffsetDateTime.now())
                .isDeleted(false)
                .version(1)
                .build();
    }

    @Test
    @DisplayName("Should successfully group records by field")
    void testGroupBy_Success() {
        // Given: Mock validator and repository
        doNothing().when(jsonValidator).validateDatasetName(anyString());
        when(datasetRepository.findByDatasetNameAndIsDeleted("users", false))
                .thenReturn(sampleRecords);

        // When: Execute group-by
        QueryResponse response = datasetService.groupBy(validQueryRequest);

        // Then: Verify response structure
        assertThat(response).isNotNull();
        assertThat(response.getSuccess()).isTrue();
        assertThat(response.getDatasetName()).isEqualTo("users");
        assertThat(response.getOperation()).isEqualTo("group_by");
        assertThat(response.getField()).isEqualTo("status");
        assertThat(response.getTotalRecords()).isEqualTo(4);

        // Verify groups
        Map<String, List<Map<String, Object>>> groups = response.getGroups();
        assertThat(groups).isNotNull();
        assertThat(groups).hasSize(3);  // active, inactive, pending
        assertThat(groups).containsKeys("active", "inactive", "pending");

        // Verify "active" group has 2 records
        assertThat(groups.get("active")).hasSize(2);
        assertThat(groups.get("active"))
                .extracting(data -> data.get("name"))
                .containsExactlyInAnyOrder("John Doe", "Jane Smith");

        // Verify "inactive" group has 1 record
        assertThat(groups.get("inactive")).hasSize(1);
        assertThat(groups.get("inactive").get(0).get("name")).isEqualTo("Bob Johnson");

        // Verify "pending" group has 1 record
        assertThat(groups.get("pending")).hasSize(1);
        assertThat(groups.get("pending").get(0).get("name")).isEqualTo("Alice Brown");

        // Verify metadata
        QueryResponse.QueryMetadata metadata = response.getMetadata();
        assertThat(metadata).isNotNull();
        assertThat(metadata.getTotalGroups()).isEqualTo(3);
        assertThat(metadata.getRecordsWithMissingField()).isEqualTo(0);
        assertThat(metadata.getRecordsWithNullField()).isEqualTo(0);
        assertThat(metadata.getGroupSizes()).containsEntry("active", 2);
        assertThat(metadata.getGroupSizes()).containsEntry("inactive", 1);
        assertThat(metadata.getGroupSizes()).containsEntry("pending", 1);
    }

    @Test
    @DisplayName("Should handle records with missing groupBy field")
    void testGroupBy_MissingField() {
        // Given: Add records without "status" field
        Map<String, Object> dataMissing1 = new HashMap<>();
        dataMissing1.put("name", "Missing User 1");
        dataMissing1.put("age", 28);
        // No "status" field
        sampleRecords.add(createRecord("users", dataMissing1));

        Map<String, Object> dataMissing2 = new HashMap<>();
        dataMissing2.put("name", "Missing User 2");
        dataMissing2.put("age", 32);
        // No "status" field
        sampleRecords.add(createRecord("users", dataMissing2));

        doNothing().when(jsonValidator).validateDatasetName(anyString());
        when(datasetRepository.findByDatasetNameAndIsDeleted("users", false))
                .thenReturn(sampleRecords);

        // When: Execute group-by
        QueryResponse response = datasetService.groupBy(validQueryRequest);

        // Then: Records without field should be grouped under "__missing__"
        assertThat(response.getGroups()).containsKey("__missing__");
        assertThat(response.getGroups().get("__missing__")).hasSize(2);
        assertThat(response.getMetadata().getRecordsWithMissingField()).isEqualTo(2);
        assertThat(response.getTotalRecords()).isEqualTo(6);  // 4 + 2 missing
    }

    @Test
    @DisplayName("Should handle records with null field values")
    void testGroupBy_NullFieldValue() {
        // Given: Add records with null "status"
        Map<String, Object> dataNull1 = new HashMap<>();
        dataNull1.put("name", "Null User 1");
        dataNull1.put("age", 40);
        dataNull1.put("status", null);  // Explicit null
        sampleRecords.add(createRecord("users", dataNull1));

        Map<String, Object> dataNull2 = new HashMap<>();
        dataNull2.put("name", "Null User 2");
        dataNull2.put("age", 50);
        dataNull2.put("status", null);  // Explicit null
        sampleRecords.add(createRecord("users", dataNull2));

        doNothing().when(jsonValidator).validateDatasetName(anyString());
        when(datasetRepository.findByDatasetNameAndIsDeleted("users", false))
                .thenReturn(sampleRecords);

        // When: Execute group-by
        QueryResponse response = datasetService.groupBy(validQueryRequest);

        // Then: Records with null value should be grouped under "__null__"
        assertThat(response.getGroups()).containsKey("__null__");
        assertThat(response.getGroups().get("__null__")).hasSize(2);
        assertThat(response.getMetadata().getRecordsWithNullField()).isEqualTo(2);
        assertThat(response.getTotalRecords()).isEqualTo(6);  // 4 + 2 null
    }

    @Test
    @DisplayName("Should handle empty dataset gracefully")
    void testGroupBy_EmptyDataset() {
        // Given: Empty dataset
        doNothing().when(jsonValidator).validateDatasetName(anyString());
        when(datasetRepository.findByDatasetNameAndIsDeleted("users", false))
                .thenReturn(Collections.emptyList());

        // When: Execute group-by
        QueryResponse response = datasetService.groupBy(validQueryRequest);

        // Then: Should return empty result (not throw exception)
        assertThat(response).isNotNull();
        assertThat(response.getSuccess()).isTrue();
        assertThat(response.getTotalRecords()).isEqualTo(0);
        assertThat(response.getGroups()).isEmpty();
        assertThat(response.getMetadata().getTotalGroups()).isEqualTo(0);
        assertThat(response.getMetadata().getRecordsWithMissingField()).isEqualTo(0);
        assertThat(response.getMetadata().getRecordsWithNullField()).isEqualTo(0);
    }

    @Test
    @DisplayName("Should throw exception when request is null")
    void testGroupBy_NullRequest() {
        // When & Then: Expect exception
        assertThatThrownBy(() -> datasetService.groupBy(null))
                .isInstanceOf(InvalidJsonException.class)
                .hasMessageContaining("Query request cannot be null");
    }

    @Test
    @DisplayName("Should throw exception when dataset name is null")
    void testGroupBy_NullDatasetName() {
        // Given: Request with null dataset name
        QueryRequest invalidRequest = QueryRequest.builder()
                .datasetName(null)
                .groupBy("status")
                .build();

        // When & Then: Expect exception
        assertThatThrownBy(() -> datasetService.groupBy(invalidRequest))
                .isInstanceOf(InvalidJsonException.class)
                .hasMessageContaining("Query request is invalid");
    }

    @Test
    @DisplayName("Should throw exception when groupBy field is null")
    void testGroupBy_NullGroupByField() {
        // Given: Request with null groupBy field
        QueryRequest invalidRequest = QueryRequest.builder()
                .datasetName("users")
                .groupBy(null)
                .build();

        // When & Then: Expect exception
        assertThatThrownBy(() -> datasetService.groupBy(invalidRequest))
                .isInstanceOf(InvalidFieldException.class)
                .hasMessageContaining("Group-by field is required");
    }

    @Test
    @DisplayName("Should group by numeric field values")
    void testGroupBy_NumericField() {
        // Given: Records with numeric "age" field
        QueryRequest ageQuery = QueryRequest.builder()
                .datasetName("users")
                .groupBy("age")
                .build();

        doNothing().when(jsonValidator).validateDatasetName(anyString());
        when(datasetRepository.findByDatasetNameAndIsDeleted("users", false))
                .thenReturn(sampleRecords);

        // When: Group by age
        QueryResponse response = datasetService.groupBy(ageQuery);

        // Then: Should group by age values (converted to string)
        assertThat(response.getGroups()).containsKeys("30", "25", "45", "35");
        assertThat(response.getGroups().get("30")).hasSize(1);
        assertThat(response.getGroups().get("25")).hasSize(1);
        assertThat(response.getMetadata().getTotalGroups()).isEqualTo(4);
    }

    @Test
    @DisplayName("Should group by boolean field values")
    void testGroupBy_BooleanField() {
        // Given: Add boolean field to records
        sampleRecords.forEach(record -> record.getData().put("isActive", true));
        sampleRecords.get(2).getData().put("isActive", false);  // Bob is inactive

        QueryRequest booleanQuery = QueryRequest.builder()
                .datasetName("users")
                .groupBy("isActive")
                .build();

        doNothing().when(jsonValidator).validateDatasetName(anyString());
        when(datasetRepository.findByDatasetNameAndIsDeleted("users", false))
                .thenReturn(sampleRecords);

        // When: Group by boolean
        QueryResponse response = datasetService.groupBy(booleanQuery);

        // Then: Should have "true" and "false" groups
        assertThat(response.getGroups()).containsKeys("true", "false");
        assertThat(response.getGroups().get("true")).hasSize(3);
        assertThat(response.getGroups().get("false")).hasSize(1);
    }

    @Test
    @DisplayName("Should handle nested field grouping")
    void testGroupBy_NestedField() {
        // Given: Add nested "address.city" field
        Map<String, Object> address1 = Map.of("city", "New York", "zip", "10001");
        Map<String, Object> address2 = Map.of("city", "Los Angeles", "zip", "90001");
        
        sampleRecords.get(0).getData().put("address", address1);
        sampleRecords.get(1).getData().put("address", address1);  // Same city
        sampleRecords.get(2).getData().put("address", address2);

        QueryRequest nestedQuery = QueryRequest.builder()
                .datasetName("users")
                .groupBy("address.city")
                .build();

        doNothing().when(jsonValidator).validateDatasetName(anyString());
        when(datasetRepository.findByDatasetNameAndIsDeleted("users", false))
                .thenReturn(sampleRecords);

        // When: Group by nested field
        QueryResponse response = datasetService.groupBy(nestedQuery);

        // Then: Should group by city
        assertThat(response.getGroups()).containsKeys("New York", "Los Angeles", "__missing__");
        assertThat(response.getGroups().get("New York")).hasSize(2);
        assertThat(response.getGroups().get("Los Angeles")).hasSize(1);
        assertThat(response.getGroups().get("__missing__")).hasSize(1);  // Record without address
    }

    @Test
    @DisplayName("Should handle array/list field values")
    void testGroupBy_ArrayField() {
        // Given: Add array field "tags"
        sampleRecords.get(0).getData().put("tags", List.of("java", "spring"));
        sampleRecords.get(1).getData().put("tags", List.of("python", "django"));
        sampleRecords.get(2).getData().put("tags", List.of("java", "spring"));  // Same tags

        QueryRequest arrayQuery = QueryRequest.builder()
                .datasetName("users")
                .groupBy("tags")
                .build();

        doNothing().when(jsonValidator).validateDatasetName(anyString());
        when(datasetRepository.findByDatasetNameAndIsDeleted("users", false))
                .thenReturn(sampleRecords);

        // When: Group by array field
        QueryResponse response = datasetService.groupBy(arrayQuery);

        // Then: Arrays should be converted to comma-separated strings
        assertThat(response.getGroups()).containsKeys("java, spring", "python, django", "__missing__");
        assertThat(response.getGroups().get("java, spring")).hasSize(2);
        assertThat(response.getGroups().get("python, django")).hasSize(1);
    }

    @Test
    @DisplayName("Should handle mixed missing and null fields")
    void testGroupBy_MixedEdgeCases() {
        // Given: Mix of normal, missing, and null
        // Record 1-4: Normal (from setUp)
        
        // Record 5: Missing field
        Map<String, Object> dataMissing = new HashMap<>();
        dataMissing.put("name", "Missing");
        sampleRecords.add(createRecord("users", dataMissing));

        // Record 6: Null field
        Map<String, Object> dataNull = new HashMap<>();
        dataNull.put("name", "Null");
        dataNull.put("status", null);
        sampleRecords.add(createRecord("users", dataNull));

        doNothing().when(jsonValidator).validateDatasetName(anyString());
        when(datasetRepository.findByDatasetNameAndIsDeleted("users", false))
                .thenReturn(sampleRecords);

        // When: Execute group-by
        QueryResponse response = datasetService.groupBy(validQueryRequest);

        // Then: Should have all groups including edge cases
        assertThat(response.getGroups()).hasSize(5);  // active, inactive, pending, __missing__, __null__
        assertThat(response.getGroups().get("__missing__")).hasSize(1);
        assertThat(response.getGroups().get("__null__")).hasSize(1);
        assertThat(response.getMetadata().getRecordsWithMissingField()).isEqualTo(1);
        assertThat(response.getMetadata().getRecordsWithNullField()).isEqualTo(1);
        assertThat(response.getTotalRecords()).isEqualTo(6);
    }

    @Test
    @DisplayName("Should handle large dataset efficiently")
    void testGroupBy_LargeDataset() {
        // Given: Generate 1000 records with 10 different statuses
        List<DatasetRecord> largeDataset = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            Map<String, Object> data = new HashMap<>();
            data.put("name", "User " + i);
            data.put("status", "status_" + (i % 10));  // 10 different statuses
            largeDataset.add(createRecord("users", data));
        }

        doNothing().when(jsonValidator).validateDatasetName(anyString());
        when(datasetRepository.findByDatasetNameAndIsDeleted("users", false))
                .thenReturn(largeDataset);

        // When: Execute group-by
        long startTime = System.currentTimeMillis();
        QueryResponse response = datasetService.groupBy(validQueryRequest);
        long endTime = System.currentTimeMillis();

        // Then: Should complete quickly and correctly
        assertThat(response.getGroups()).hasSize(10);
        assertThat(response.getTotalRecords()).isEqualTo(1000);
        assertThat(response.getMetadata().getTotalGroups()).isEqualTo(10);
        
        // Each group should have 100 records
        response.getGroups().forEach((key, records) -> {
            assertThat(records).hasSize(100);
        });

        // Performance check: should complete in < 1 second
        long executionTime = endTime - startTime;
        assertThat(executionTime).isLessThan(1000);
        System.out.println("Group-by on 1000 records took: " + executionTime + "ms");
    }
}
