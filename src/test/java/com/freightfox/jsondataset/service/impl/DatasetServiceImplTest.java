package com.freightfox.jsondataset.service.impl;

import com.freightfox.jsondataset.dto.request.InsertRecordRequest;
import com.freightfox.jsondataset.dto.response.InsertRecordResponse;
import com.freightfox.jsondataset.exception.DatasetValidationException;
import com.freightfox.jsondataset.exception.InvalidJsonException;
import com.freightfox.jsondataset.model.entity.DatasetRecord;
import com.freightfox.jsondataset.repository.DatasetRepository;
import com.freightfox.jsondataset.util.JsonValidator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.OffsetDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.*;

/**
 * Unit tests for DatasetServiceImpl using Mockito.
 * 
 * @ExtendWith(MockitoExtension.class):
 * - Enables Mockito annotations
 * - Initializes mocks before each test
 * - Validates mock usage after each test
 * 
 * WHY UNIT TESTS WITH MOCKS?
 * - Fast (no database, no Spring context)
 * - Isolated (tests only service logic)
 * - Focused (one method per test)
 * - Repeatable (no external dependencies)
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("DatasetService Unit Tests")
class DatasetServiceImplTest {

    /**
     * @Mock creates a mock instance
     * Mock = fake object that records interactions
     */
    @Mock
    private DatasetRepository datasetRepository;

    @Mock
    private JsonValidator jsonValidator;

    /**
     * @InjectMocks creates instance and injects mocks
     * This is the actual class under test
     */
    @InjectMocks
    private DatasetServiceImpl datasetService;

    /**
     * @Captor captures arguments passed to mocked methods
     * Useful for verifying exact values passed to repository
     */
    @Captor
    private ArgumentCaptor<DatasetRecord> recordCaptor;

    private InsertRecordRequest validRequest;
    private Map<String, Object> validData;
    private DatasetRecord savedRecord;

    @BeforeEach
    void setUp() {
        // Setup valid test data
        validData = new HashMap<>();
        validData.put("name", "John Doe");
        validData.put("age", 30);
        validData.put("status", "active");

        validRequest = InsertRecordRequest.builder()
                .datasetName("users")
                .data(validData)
                .build();

        savedRecord = DatasetRecord.builder()
                .id(UUID.randomUUID())
                .datasetName("users")
                .data(validData)
                .createdAt(OffsetDateTime.now())
                .isDeleted(false)
                .version(1)
                .build();
    }

    @Test
    @DisplayName("Should successfully insert valid record")
    void testInsertRecord_Success() {
        // Given: Mock validator to do nothing (pass validation)
        doNothing().when(jsonValidator).validateDatasetName(anyString());
        doNothing().when(jsonValidator).validateJsonData(any(), anyBoolean());

        // Mock repository to return saved record
        when(datasetRepository.save(any(DatasetRecord.class))).thenReturn(savedRecord);

        // When: Insert record
        InsertRecordResponse response = datasetService.insertRecord(validRequest);

        // Then: Verify response
        assertThat(response).isNotNull();
        assertThat(response.getSuccess()).isTrue();
        assertThat(response.getMessage()).isEqualTo("Record inserted successfully");
        assertThat(response.getRecordId()).isEqualTo(savedRecord.getId());
        assertThat(response.getDatasetName()).isEqualTo("users");
        assertThat(response.getCreatedAt()).isNotNull();

        // Verify interactions
        verify(jsonValidator).validateDatasetName("users");
        verify(jsonValidator).validateJsonData(validData, true);
        verify(datasetRepository).save(any(DatasetRecord.class));

        // Verify the exact entity passed to repository
        verify(datasetRepository).save(recordCaptor.capture());
        DatasetRecord capturedRecord = recordCaptor.getValue();
        assertThat(capturedRecord.getDatasetName()).isEqualTo("users");
        assertThat(capturedRecord.getData()).containsAllEntriesOf(validData);
        assertThat(capturedRecord.getIsDeleted()).isFalse();
    }

    @Test
    @DisplayName("Should throw exception when request is null")
    void testInsertRecord_NullRequest() {
        // When & Then: Expect exception
        assertThatThrownBy(() -> datasetService.insertRecord(null))
                .isInstanceOf(InvalidJsonException.class)
                .hasMessageContaining("Insert request cannot be null");

        // Verify no repository interaction
        verify(datasetRepository, never()).save(any());
    }

    @Test
    @DisplayName("Should throw exception when dataset name is null")
    void testInsertRecord_NullDatasetName() {
        // Given: Request with null dataset name
        InsertRecordRequest invalidRequest = InsertRecordRequest.builder()
                .datasetName(null)
                .data(validData)
                .build();

        // When & Then: Expect exception
        assertThatThrownBy(() -> datasetService.insertRecord(invalidRequest))
                .isInstanceOf(InvalidJsonException.class)
                .hasMessageContaining("Insert request is invalid");

        // Verify no repository interaction
        verify(datasetRepository, never()).save(any());
    }

    @Test
    @DisplayName("Should throw exception when dataset name is blank")
    void testInsertRecord_BlankDatasetName() {
        // Given: Request with blank dataset name
        InsertRecordRequest invalidRequest = InsertRecordRequest.builder()
                .datasetName("   ")
                .data(validData)
                .build();

        // When & Then: Expect exception
        assertThatThrownBy(() -> datasetService.insertRecord(invalidRequest))
                .isInstanceOf(InvalidJsonException.class)
                .hasMessageContaining("Insert request is invalid");

        verify(datasetRepository, never()).save(any());
    }

    @Test
    @DisplayName("Should throw exception when data is null")
    void testInsertRecord_NullData() {
        // Given: Request with null data
        InsertRecordRequest invalidRequest = InsertRecordRequest.builder()
                .datasetName("users")
                .data(null)
                .build();

        // When & Then: Expect exception
        assertThatThrownBy(() -> datasetService.insertRecord(invalidRequest))
                .isInstanceOf(InvalidJsonException.class)
                .hasMessageContaining("Insert request is invalid");

        verify(datasetRepository, never()).save(any());
    }

    @Test
    @DisplayName("Should throw exception when dataset name is invalid")
    void testInsertRecord_InvalidDatasetName() {
        // Given: Mock validator to throw exception
        doThrow(new InvalidJsonException(
                "Dataset name validation failed",
                List.of("Invalid characters in dataset name")
        ))
        .when(jsonValidator).validateDatasetName(anyString());

        // When & Then: Expect DatasetValidationException
        assertThatThrownBy(() -> datasetService.insertRecord(validRequest))
                .isInstanceOf(DatasetValidationException.class)
                .hasMessageContaining("Dataset name validation failed");

        verify(datasetRepository, never()).save(any());
    }

    @Test
    @DisplayName("Should throw exception when JSON data is invalid")
    void testInsertRecord_InvalidJson() {
        // Given: Mock validators
        doNothing().when(jsonValidator).validateDatasetName(anyString());
        doThrow(new InvalidJsonException(
                "JSON validation failed",
                List.of("JSON contains circular reference")
        ))
        .when(jsonValidator).validateJsonData(any(), anyBoolean());

        // When & Then: Expect InvalidJsonException
        assertThatThrownBy(() -> datasetService.insertRecord(validRequest))
                .isInstanceOf(InvalidJsonException.class)
                .hasMessageContaining("JSON validation failed");

        verify(datasetRepository, never()).save(any());
    }

    @Test
    @DisplayName("Should handle empty JSON data gracefully")
    void testInsertRecord_EmptyJson() {
        // Given: Request with empty map
        Map<String, Object> emptyData = new HashMap<>();
        InsertRecordRequest emptyRequest = InsertRecordRequest.builder()
                .datasetName("users")
                .data(emptyData)
                .build();

        DatasetRecord emptyRecord = DatasetRecord.builder()
                .id(UUID.randomUUID())
                .datasetName("users")
                .data(emptyData)
                .createdAt(OffsetDateTime.now())
                .isDeleted(false)
                .version(1)
                .build();

        // Mock validators and repository
        doNothing().when(jsonValidator).validateDatasetName(anyString());
        doNothing().when(jsonValidator).validateJsonData(any(), anyBoolean());
        when(datasetRepository.save(any(DatasetRecord.class))).thenReturn(emptyRecord);

        // When: Insert record with empty data
        InsertRecordResponse response = datasetService.insertRecord(emptyRequest);

        // Then: Should succeed (empty maps are allowed)
        assertThat(response).isNotNull();
        assertThat(response.getSuccess()).isTrue();
        verify(datasetRepository).save(any(DatasetRecord.class));
    }

    @Test
    @DisplayName("Should handle repository save failure")
    void testInsertRecord_RepositorySaveFailure() {
        // Given: Mock validators to pass
        doNothing().when(jsonValidator).validateDatasetName(anyString());
        doNothing().when(jsonValidator).validateJsonData(any(), anyBoolean());

        // Mock repository to throw exception
        when(datasetRepository.save(any(DatasetRecord.class)))
                .thenThrow(new RuntimeException("Database connection failed"));

        // When & Then: Expect RuntimeException
        assertThatThrownBy(() -> datasetService.insertRecord(validRequest))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Failed to insert record");

        verify(datasetRepository).save(any(DatasetRecord.class));
    }

    @Test
    @DisplayName("Should create defensive copy of data map")
    void testInsertRecord_DefensiveCopy() {
        // Given: Mocks
        doNothing().when(jsonValidator).validateDatasetName(anyString());
        doNothing().when(jsonValidator).validateJsonData(any(), anyBoolean());
        when(datasetRepository.save(any(DatasetRecord.class))).thenReturn(savedRecord);

        // When: Insert record
        datasetService.insertRecord(validRequest);

        // Then: Verify repository received a copy (not the original map)
        verify(datasetRepository).save(recordCaptor.capture());
        DatasetRecord capturedRecord = recordCaptor.getValue();

        // Modify original data
        validData.put("modified", true);

        // Captured record should not have the modification
        assertThat(capturedRecord.getData()).doesNotContainKey("modified");
    }

    @Test
    @DisplayName("Should handle complex nested JSON structures")
    void testInsertRecord_NestedJson() {
        // Given: Complex nested structure
        Map<String, Object> address = new HashMap<>();
        address.put("city", "New York");
        address.put("zip", "10001");

        Map<String, Object> nestedData = new HashMap<>();
        nestedData.put("user", "John");
        nestedData.put("address", address);
        nestedData.put("tags", List.of("java", "spring", "postgres"));

        InsertRecordRequest nestedRequest = InsertRecordRequest.builder()
                .datasetName("users")
                .data(nestedData)
                .build();

        DatasetRecord nestedRecord = DatasetRecord.builder()
                .id(UUID.randomUUID())
                .datasetName("users")
                .data(nestedData)
                .createdAt(OffsetDateTime.now())
                .isDeleted(false)
                .version(1)
                .build();

        // Mock
        doNothing().when(jsonValidator).validateDatasetName(anyString());
        doNothing().when(jsonValidator).validateJsonData(any(), anyBoolean());
        when(datasetRepository.save(any(DatasetRecord.class))).thenReturn(nestedRecord);

        // When: Insert nested structure
        InsertRecordResponse response = datasetService.insertRecord(nestedRequest);

        // Then: Should succeed
        assertThat(response).isNotNull();
        assertThat(response.getSuccess()).isTrue();
        verify(datasetRepository).save(any(DatasetRecord.class));
    }
}