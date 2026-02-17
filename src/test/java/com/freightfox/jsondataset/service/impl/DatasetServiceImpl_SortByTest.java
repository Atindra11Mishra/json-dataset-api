package com.freightfox.jsondataset.service.impl;

import com.freightfox.jsondataset.dto.request.QueryRequest;
import com.freightfox.jsondataset.dto.response.QueryResponse;
import com.freightfox.jsondataset.exception.InvalidFieldException;
import com.freightfox.jsondataset.exception.InvalidSortOrderException;
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
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

/**
 * Unit tests for sort-by functionality in DatasetServiceImpl.
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("DatasetService Sort-By Tests")
class DatasetServiceImpl_SortByTest {

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

    private DatasetRecord createRecord(Object value) {
        Map<String, Object> data = new HashMap<>();
        data.put("field", value);
        return createRecordWithData(data);
    }

    private DatasetRecord createRecordWithData(Map<String, Object> data) {
        return DatasetRecord.builder()
                .id(UUID.randomUUID())
                .datasetName("test_dataset")
                .data(data)
                .createdAt(OffsetDateTime.now())
                .isDeleted(false)
                .version(1)
                .build();
    }

    @Test
    @DisplayName("Should sort numeric values ascending")
    void testSortBy_NumericAsc() {
        sampleRecords.add(createRecord(10));
        sampleRecords.add(createRecord(5));
        sampleRecords.add(createRecord(20));

        QueryRequest request = QueryRequest.builder()
                .datasetName("test_dataset")
                .sortBy("field")
                .sortOrder("asc")
                .build();

        doNothing().when(jsonValidator).validateDatasetName(anyString());
        when(datasetRepository.findByDatasetNameAndIsDeleted(anyString(), eq(false)))
                .thenReturn(sampleRecords);

        QueryResponse response = datasetService.sortBy(request);

        assertThat(response.getResults())
                .extracting(map -> map.get("field"))
                .containsExactly(5, 10, 20);
        assertThat(response.getSortMetadata().getFieldType()).isEqualTo("numeric");
    }

    @Test
    @DisplayName("Should sort numeric values descending")
    void testSortBy_NumericDesc() {
        sampleRecords.add(createRecord(10));
        sampleRecords.add(createRecord(5));
        sampleRecords.add(createRecord(20));

        QueryRequest request = QueryRequest.builder()
                .datasetName("test_dataset")
                .sortBy("field")
                .sortOrder("desc")
                .build();

        doNothing().when(jsonValidator).validateDatasetName(anyString());
        when(datasetRepository.findByDatasetNameAndIsDeleted(anyString(), eq(false)))
                .thenReturn(sampleRecords);

        QueryResponse response = datasetService.sortBy(request);

        assertThat(response.getResults())
                .extracting(map -> map.get("field"))
                .containsExactly(20, 10, 5);
    }

    @Test
    @DisplayName("Should sort string values case-insensitive")
    void testSortBy_String() {
        sampleRecords.add(createRecord("Apple"));
        sampleRecords.add(createRecord("banana")); // Lowercase
        sampleRecords.add(createRecord("CHERRY")); // Uppercase

        QueryRequest request = QueryRequest.builder()
                .datasetName("test_dataset")
                .sortBy("field")
                .sortOrder("asc")
                .build();

        doNothing().when(jsonValidator).validateDatasetName(anyString());
        when(datasetRepository.findByDatasetNameAndIsDeleted(anyString(), eq(false)))
                .thenReturn(sampleRecords);

        QueryResponse response = datasetService.sortBy(request);

        assertThat(response.getResults())
                .extracting(map -> map.get("field"))
                .containsExactly("Apple", "banana", "CHERRY"); // A, b, c (case insensitive)
        assertThat(response.getSortMetadata().getFieldType()).isEqualTo("string");
    }

    @Test
    @DisplayName("Should sort nulls to the end in ASC order")
    void testSortBy_NullsAsc() {
        sampleRecords.add(createRecord(10));
        sampleRecords.add(createRecord(null));
        sampleRecords.add(createRecord(5));

        QueryRequest request = QueryRequest.builder()
                .datasetName("test_dataset")
                .sortBy("field")
                .sortOrder("asc")
                .build();

        doNothing().when(jsonValidator).validateDatasetName(anyString());
        when(datasetRepository.findByDatasetNameAndIsDeleted(anyString(), eq(false)))
                .thenReturn(sampleRecords);

        QueryResponse response = datasetService.sortBy(request);

        assertThat(response.getResults())
                .extracting(map -> map.get("field"))
                .containsExactly(5, 10, null);
        assertThat(response.getSortMetadata().getRecordsWithNullField()).isEqualTo(1);
    }

    @Test
    @DisplayName("Should sort nulls to the end in DESC order")
    void testSortBy_NullsDesc() {
        sampleRecords.add(createRecord(10));
        sampleRecords.add(createRecord(null));
        sampleRecords.add(createRecord(5));

        QueryRequest request = QueryRequest.builder()
                .datasetName("test_dataset")
                .sortBy("field")
                .sortOrder("desc")
                .build();

        doNothing().when(jsonValidator).validateDatasetName(anyString());
        when(datasetRepository.findByDatasetNameAndIsDeleted(anyString(), eq(false)))
                .thenReturn(sampleRecords);

        QueryResponse response = datasetService.sortBy(request);

        // Nulls always at the end!
        assertThat(response.getResults())
                .extracting(map -> map.get("field"))
                .containsExactly(10, 5, null);
    }

    @Test
    @DisplayName("Should handle missing fields as nulls")
    void testSortBy_MissingFields() {
        sampleRecords.add(createRecord(10));

        Map<String, Object> missingData = new HashMap<>();
        missingData.put("other", "value");
        sampleRecords.add(createRecordWithData(missingData));

        sampleRecords.add(createRecord(5));

        QueryRequest request = QueryRequest.builder()
                .datasetName("test_dataset")
                .sortBy("field")
                .sortOrder("asc")
                .build();

        doNothing().when(jsonValidator).validateDatasetName(anyString());
        when(datasetRepository.findByDatasetNameAndIsDeleted(anyString(), eq(false)))
                .thenReturn(sampleRecords);

        QueryResponse response = datasetService.sortBy(request);

        assertThat(response.getResults())
                .extracting(map -> map.get("field"))
                .containsExactly(5, 10, null);
        assertThat(response.getSortMetadata().getRecordsWithMissingField()).isEqualTo(1);
    }

    @Test
    @DisplayName("Should handle mixed types by sorting as strings")
    void testSortBy_MixedTypes() {
        sampleRecords.add(createRecord(10));
        sampleRecords.add(createRecord("20"));
        sampleRecords.add(createRecord("5"));

        QueryRequest request = QueryRequest.builder()
                .datasetName("test_dataset")
                .sortBy("field")
                .sortOrder("asc")
                .build();

        doNothing().when(jsonValidator).validateDatasetName(anyString());
        when(datasetRepository.findByDatasetNameAndIsDeleted(anyString(), eq(false)))
                .thenReturn(sampleRecords);

        QueryResponse response = datasetService.sortBy(request);

        // Sorted as strings: "10", "20", "5" -> "10", "20", "5"
        // Wait, "10" < "20" < "5" (correct for string sort)
        assertThat(response.getResults())
                .extracting(map -> map.get("field"))
                .containsExactly(10, "20", "5");

        assertThat(response.getSortMetadata().getFieldType()).isEqualTo("mixed");
        assertThat(response.getSortMetadata().getWarnings()).isNotEmpty();
    }

    @Test
    @DisplayName("Should extract nested fields")
    void testSortBy_NestedField() {
        Map<String, Object> data1 = new HashMap<>();
        data1.put("address", Map.of("zip", 10001));
        sampleRecords.add(createRecordWithData(data1));

        Map<String, Object> data2 = new HashMap<>();
        data2.put("address", Map.of("zip", 90001));
        sampleRecords.add(createRecordWithData(data2));

        Map<String, Object> data3 = new HashMap<>();
        data3.put("address", Map.of("zip", 50000));
        sampleRecords.add(createRecordWithData(data3));

        QueryRequest request = QueryRequest.builder()
                .datasetName("test_dataset")
                .sortBy("address.zip")
                .sortOrder("asc")
                .build();

        doNothing().when(jsonValidator).validateDatasetName(anyString());
        when(datasetRepository.findByDatasetNameAndIsDeleted(anyString(), eq(false)))
                .thenReturn(sampleRecords);

        QueryResponse response = datasetService.sortBy(request);

        assertThat(response.getResults())
                .extracting(map -> ((Map) map.get("address")).get("zip"))
                .containsExactly(10001, 50000, 90001);
    }
}