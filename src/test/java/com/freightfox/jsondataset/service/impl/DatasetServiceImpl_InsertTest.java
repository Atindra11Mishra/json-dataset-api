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
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.OffsetDateTime;
import java.util.*;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Comprehensive unit tests for Insert operation in DatasetServiceImpl.
 * 
 * TEST ORGANIZATION:
 * - @Nested classes group related tests
 * - Given-When-Then pattern for clarity
 * - ArgumentCaptor to verify exact values passed to repository
 * 
 * MOCKITO PATTERNS:
 * - @Mock: Create mock objects
 * - @InjectMocks: Inject mocks into service
 * - @Captor: Capture arguments passed to mocks
 * 
 * ASSERTION STRATEGIES:
 * - AssertJ for fluent assertions
 * - Specific exception message verification
 * - Verify mock interactions (times, order, arguments)
 * 
 * COVERAGE TARGET: 95%+
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("DatasetService Insert Operation Tests")
class DatasetServiceImpl_InsertTest {

    @Mock
    private DatasetRepository datasetRepository;

    @Mock
    private JsonValidator jsonValidator;

    @InjectMocks
    private DatasetServiceImpl datasetService;

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
                .updatedAt(OffsetDateTime.now())
                .isDeleted(false)
                .version(1)
                .build();
    }

    // ==================== SUCCESS SCENARIOS ====================

    @Nested
    @DisplayName("Success Scenarios")
    class SuccessScenarios {

        @Test
        @DisplayName("Should successfully insert record with valid data")
        void testInsertRecord_Success() {
            // Given: Mocks configured for success
            doNothing().when(jsonValidator).validateDatasetName(anyString());
            doNothing().when(jsonValidator).validateJsonData(any(), anyBoolean());
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

            // Verify mock interactions
            verify(jsonValidator).validateDatasetName("users");
            verify(jsonValidator).validateJsonData(validData, true);
            verify(datasetRepository).save(any(DatasetRecord.class));
        }

        @Test
        @DisplayName("Should insert record with empty data map")
        void testInsertRecord_EmptyData() {
            // Given: Empty data map
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
                    .updatedAt(OffsetDateTime.now())
                    .isDeleted(false)
                    .version(1)
                    .build();

            doNothing().when(jsonValidator).validateDatasetName(anyString());
            doNothing().when(jsonValidator).validateJsonData(any(), anyBoolean());
            when(datasetRepository.save(any(DatasetRecord.class))).thenReturn(emptyRecord);

            // When: Insert with empty data
            InsertRecordResponse response = datasetService.insertRecord(emptyRequest);

            // Then: Should succeed (empty maps allowed)
            assertThat(response.getSuccess()).isTrue();
            verify(datasetRepository).save(any(DatasetRecord.class));
        }

        @Test
        @DisplayName("Should handle nested JSON structures")
        void testInsertRecord_NestedData() {
            // Given: Nested data structure
            Map<String, Object> address = new HashMap<>();
            address.put("city", "New York");
            address.put("zip", "10001");

            Map<String, Object> nestedData = new HashMap<>();
            nestedData.put("name", "John");
            nestedData.put("address", address);
            nestedData.put("tags", List.of("developer", "java"));

            InsertRecordRequest nestedRequest = InsertRecordRequest.builder()
                    .datasetName("users")
                    .data(nestedData)
                    .build();

            DatasetRecord nestedRecord = DatasetRecord.builder()
                    .id(UUID.randomUUID())
                    .datasetName("users")
                    .data(nestedData)
                    .createdAt(OffsetDateTime.now())
                    .updatedAt(OffsetDateTime.now())
                    .isDeleted(false)
                    .version(1)
                    .build();

            doNothing().when(jsonValidator).validateDatasetName(anyString());
            doNothing().when(jsonValidator).validateJsonData(any(), anyBoolean());
            when(datasetRepository.save(any(DatasetRecord.class))).thenReturn(nestedRecord);

            // When: Insert nested structure
            InsertRecordResponse response = datasetService.insertRecord(nestedRequest);

            // Then: Should succeed
            assertThat(response.getSuccess()).isTrue();
            verify(datasetRepository).save(any(DatasetRecord.class));
        }

        @Test
        @DisplayName("Should create defensive copy of data map")
        void testInsertRecord_DefensiveCopy() {
            // Given: Mocks configured
            doNothing().when(jsonValidator).validateDatasetName(anyString());
            doNothing().when(jsonValidator).validateJsonData(any(), anyBoolean());
            when(datasetRepository.save(any(DatasetRecord.class))).thenReturn(savedRecord);

            // When: Insert record
            datasetService.insertRecord(validRequest);

            // Then: Verify repository received a copy (not original map)
            verify(datasetRepository).save(recordCaptor.capture());
            DatasetRecord capturedRecord = recordCaptor.getValue();

            // Modify original data
            validData.put("modified", true);

            // Captured record should not have modification (defensive copy worked)
            assertThat(capturedRecord.getData()).doesNotContainKey("modified");
        }

        @Test
        @DisplayName("Should handle various data types in JSON")
        void testInsertRecord_VariousDataTypes() {
            // Given: Data with various types
            Map<String, Object> mixedData = new HashMap<>();
            mixedData.put("string", "text");
            mixedData.put("integer", 42);
            mixedData.put("double", 3.14);
            mixedData.put("boolean", true);
            mixedData.put("null", null);
            mixedData.put("array", List.of(1, 2, 3));
            mixedData.put("nested", Map.of("key", "value"));

            InsertRecordRequest mixedRequest = InsertRecordRequest.builder()
                    .datasetName("users")
                    .data(mixedData)
                    .build();

            DatasetRecord mixedRecord = DatasetRecord.builder()
                    .id(UUID.randomUUID())
                    .datasetName("users")
                    .data(mixedData)
                    .createdAt(OffsetDateTime.now())
                    .updatedAt(OffsetDateTime.now())
                    .isDeleted(false)
                    .version(1)
                    .build();

            doNothing().when(jsonValidator).validateDatasetName(anyString());
            doNothing().when(jsonValidator).validateJsonData(any(), anyBoolean());
            when(datasetRepository.save(any(DatasetRecord.class))).thenReturn(mixedRecord);

            // When: Insert mixed types
            InsertRecordResponse response = datasetService.insertRecord(mixedRequest);

            // Then: Should succeed
            assertThat(response.getSuccess()).isTrue();
        }
    }

    // ==================== NULL/INVALID REQUEST ====================

    @Nested
    @DisplayName("Invalid Request Scenarios")
    class InvalidRequestScenarios {

        @Test
        @DisplayName("Should throw exception when request is null")
        void testInsertRecord_NullRequest() {
            // When & Then
            assertThatThrownBy(() -> datasetService.insertRecord(null))
                    .isInstanceOf(InvalidJsonException.class)
                    .hasMessageContaining("Insert request cannot be null");

            // Verify no repository interaction
            verifyNoInteractions(datasetRepository);
        }

        @Test
        @DisplayName("Should throw exception when dataset name is null")
        void testInsertRecord_NullDatasetName() {
            // Given: Request with null dataset name
            InsertRecordRequest invalidRequest = InsertRecordRequest.builder()
                    .datasetName(null)
                    .data(validData)
                    .build();

            // When & Then
            assertThatThrownBy(() -> datasetService.insertRecord(invalidRequest))
                    .isInstanceOf(InvalidJsonException.class)
                    .hasMessageContaining("Insert request is invalid");

            verifyNoInteractions(datasetRepository);
        }

        @Test
        @DisplayName("Should throw exception when dataset name is blank")
        void testInsertRecord_BlankDatasetName() {
            // Given: Request with blank dataset name
            InsertRecordRequest invalidRequest = InsertRecordRequest.builder()
                    .datasetName("   ")
                    .data(validData)
                    .build();

            // When & Then
            assertThatThrownBy(() -> datasetService.insertRecord(invalidRequest))
                    .isInstanceOf(InvalidJsonException.class)
                    .hasMessageContaining("Insert request is invalid");

            verifyNoInteractions(datasetRepository);
        }

        @Test
        @DisplayName("Should throw exception when data is null")
        void testInsertRecord_NullData() {
            // Given: Request with null data
            InsertRecordRequest invalidRequest = InsertRecordRequest.builder()
                    .datasetName("users")
                    .data(null)
                    .build();

            // When & Then
            assertThatThrownBy(() -> datasetService.insertRecord(invalidRequest))
                    .isInstanceOf(InvalidJsonException.class)
                    .hasMessageContaining("Insert request is invalid");

            verifyNoInteractions(datasetRepository);
        }
    }

    // ==================== VALIDATION ERRORS ====================

    @Nested
    @DisplayName("Validation Error Scenarios")
    class ValidationErrorScenarios {

        @Test
        @DisplayName("Should throw exception when dataset name validation fails")
        void testInsertRecord_InvalidDatasetName() {
            // Given: Validator throws exception for invalid name
            doThrow(new InvalidJsonException(
                    "Dataset name validation failed",
                    List.of("Invalid characters in dataset name")
            ))
            .when(jsonValidator).validateDatasetName(anyString());

            // When & Then
            assertThatThrownBy(() -> datasetService.insertRecord(validRequest))
                    .isInstanceOf(DatasetValidationException.class)
                    .hasMessageContaining("Dataset name validation failed");

            // Verify validator called but repository not called
            verify(jsonValidator).validateDatasetName("users");
            verifyNoInteractions(datasetRepository);
        }

        @Test
        @DisplayName("Should throw exception when JSON data validation fails")
        void testInsertRecord_InvalidJsonData() {
            // Given: Validators configured
            doNothing().when(jsonValidator).validateDatasetName(anyString());
            doThrow(new InvalidJsonException(
                    "JSON validation failed",
                    List.of("JSON contains circular reference")
            ))
            .when(jsonValidator).validateJsonData(any(), anyBoolean());

            // When & Then
            assertThatThrownBy(() -> datasetService.insertRecord(validRequest))
                    .isInstanceOf(InvalidJsonException.class)
                    .hasMessageContaining("JSON validation failed")
                    .satisfies(ex -> {
                        InvalidJsonException jsonEx = (InvalidJsonException) ex;
                        assertThat(jsonEx.getValidationErrors())
                                .contains("JSON contains circular reference");
                    });

            verify(jsonValidator).validateDatasetName("users");
            verify(jsonValidator).validateJsonData(validData, true);
            verifyNoInteractions(datasetRepository);
        }

        @Test
        @DisplayName("Should throw exception for dataset name too long")
        void testInsertRecord_DatasetNameTooLong() {
            // Given: Name > 255 characters
            String longName = "a".repeat(256);
            InsertRecordRequest longNameRequest = InsertRecordRequest.builder()
                    .datasetName(longName)
                    .data(validData)
                    .build();

            doThrow(new InvalidJsonException("Dataset name exceeds maximum length"))
                    .when(jsonValidator).validateDatasetName(longName);

            // When & Then
            assertThatThrownBy(() -> datasetService.insertRecord(longNameRequest))
                    .isInstanceOf(DatasetValidationException.class)
                    .hasMessageContaining("maximum length");

            verifyNoInteractions(datasetRepository);
        }

        @Test
        @DisplayName("Should throw exception for dataset name with special characters")
        void testInsertRecord_DatasetNameSpecialChars() {
            // Given: Name with special characters
            InsertRecordRequest specialCharsRequest = InsertRecordRequest.builder()
                    .datasetName("invalid@name!")
                    .data(validData)
                    .build();

            doThrow(new InvalidJsonException("Invalid characters in dataset name"))
                    .when(jsonValidator).validateDatasetName("invalid@name!");

            // When & Then
            assertThatThrownBy(() -> datasetService.insertRecord(specialCharsRequest))
                    .isInstanceOf(DatasetValidationException.class);

            verifyNoInteractions(datasetRepository);
        }
    }

    // ==================== REPOSITORY ERRORS ====================

    @Nested
    @DisplayName("Repository Error Scenarios")
    class RepositoryErrorScenarios {

        @Test
        @DisplayName("Should wrap repository exception in RuntimeException")
        void testInsertRecord_RepositorySaveFailure() {
            // Given: Repository throws exception
            doNothing().when(jsonValidator).validateDatasetName(anyString());
            doNothing().when(jsonValidator).validateJsonData(any(), anyBoolean());
            when(datasetRepository.save(any(DatasetRecord.class)))
                    .thenThrow(new RuntimeException("Database connection failed"));

            // When & Then
            assertThatThrownBy(() -> datasetService.insertRecord(validRequest))
                    .isInstanceOf(RuntimeException.class)
                    .hasMessageContaining("Failed to insert record")
                    .hasCauseInstanceOf(RuntimeException.class);

            verify(datasetRepository).save(any(DatasetRecord.class));
        }

        @Test
        @DisplayName("Should handle constraint violation from database")
        void testInsertRecord_ConstraintViolation() {
            // Given: Repository throws constraint violation
            doNothing().when(jsonValidator).validateDatasetName(anyString());
            doNothing().when(jsonValidator).validateJsonData(any(), anyBoolean());
            when(datasetRepository.save(any(DatasetRecord.class)))
                    .thenThrow(new RuntimeException("Unique constraint violation"));

            // When & Then
            assertThatThrownBy(() -> datasetService.insertRecord(validRequest))
                    .isInstanceOf(RuntimeException.class)
                    .hasMessageContaining("Failed to insert record");
        }
    }

    // ==================== ENTITY CREATION VERIFICATION ====================

    @Nested
    @DisplayName("Entity Creation Verification")
    class EntityCreationVerification {

        @Test
        @DisplayName("Should set correct dataset name in entity")
        void testInsertRecord_CorrectDatasetName() {
            // Given
            doNothing().when(jsonValidator).validateDatasetName(anyString());
            doNothing().when(jsonValidator).validateJsonData(any(), anyBoolean());
            when(datasetRepository.save(any(DatasetRecord.class))).thenReturn(savedRecord);

            // When
            datasetService.insertRecord(validRequest);

            // Then: Verify exact entity passed to repository
            verify(datasetRepository).save(recordCaptor.capture());
            DatasetRecord captured = recordCaptor.getValue();
            
            assertThat(captured.getDatasetName()).isEqualTo("users");
        }

        @Test
        @DisplayName("Should set isDeleted to false in entity")
        void testInsertRecord_IsDeletedFalse() {
            // Given
            doNothing().when(jsonValidator).validateDatasetName(anyString());
            doNothing().when(jsonValidator).validateJsonData(any(), anyBoolean());
            when(datasetRepository.save(any(DatasetRecord.class))).thenReturn(savedRecord);

            // When
            datasetService.insertRecord(validRequest);

            // Then
            verify(datasetRepository).save(recordCaptor.capture());
            DatasetRecord captured = recordCaptor.getValue();
            
            assertThat(captured.getIsDeleted()).isFalse();
        }

        @Test
        @DisplayName("Should preserve all data fields in entity")
        void testInsertRecord_PreserveDataFields() {
            // Given
            doNothing().when(jsonValidator).validateDatasetName(anyString());
            doNothing().when(jsonValidator).validateJsonData(any(), anyBoolean());
            when(datasetRepository.save(any(DatasetRecord.class))).thenReturn(savedRecord);

            // When
            datasetService.insertRecord(validRequest);

            // Then
            verify(datasetRepository).save(recordCaptor.capture());
            DatasetRecord captured = recordCaptor.getValue();
            
            assertThat(captured.getData())
                    .containsEntry("name", "John Doe")
                    .containsEntry("age", 30)
                    .containsEntry("status", "active")
                    .hasSize(3);
        }

        @Test
        @DisplayName("Should not set ID in entity (database generates it)")
        void testInsertRecord_NoIdSet() {
            // Given
            doNothing().when(jsonValidator).validateDatasetName(anyString());
            doNothing().when(jsonValidator).validateJsonData(any(), anyBoolean());
            when(datasetRepository.save(any(DatasetRecord.class))).thenReturn(savedRecord);

            // When
            datasetService.insertRecord(validRequest);

            // Then
            verify(datasetRepository).save(recordCaptor.capture());
            DatasetRecord captured = recordCaptor.getValue();
            
            // ID should be null before save (database generates it)
            assertThat(captured.getId()).isNull();
        }
    }

    // ==================== MOCK INTERACTION VERIFICATION ====================

    @Nested
    @DisplayName("Mock Interaction Verification")
    class MockInteractionVerification {

        @Test
        @DisplayName("Should call validators in correct order")
        void testInsertRecord_ValidatorCallOrder() {
            // Given
            doNothing().when(jsonValidator).validateDatasetName(anyString());
            doNothing().when(jsonValidator).validateJsonData(any(), anyBoolean());
            when(datasetRepository.save(any(DatasetRecord.class))).thenReturn(savedRecord);

            // When
            datasetService.insertRecord(validRequest);

            // Then: Verify call order
            var inOrder = inOrder(jsonValidator, datasetRepository);
            inOrder.verify(jsonValidator).validateDatasetName("users");
            inOrder.verify(jsonValidator).validateJsonData(validData, true);
            inOrder.verify(datasetRepository).save(any(DatasetRecord.class));
        }

        @Test
        @DisplayName("Should call validators exactly once")
        void testInsertRecord_ValidatorCallCount() {
            // Given
            doNothing().when(jsonValidator).validateDatasetName(anyString());
            doNothing().when(jsonValidator).validateJsonData(any(), anyBoolean());
            when(datasetRepository.save(any(DatasetRecord.class))).thenReturn(savedRecord);

            // When
            datasetService.insertRecord(validRequest);

            // Then: Verify each validator called exactly once
            verify(jsonValidator, times(1)).validateDatasetName("users");
            verify(jsonValidator, times(1)).validateJsonData(validData, true);
            verify(datasetRepository, times(1)).save(any(DatasetRecord.class));
        }

        @Test
        @DisplayName("Should not call repository when validation fails")
        void testInsertRecord_NoRepositoryCallOnValidationFailure() {
            // Given: Validation fails
            doThrow(new InvalidJsonException("Validation failed"))
                    .when(jsonValidator).validateDatasetName(anyString());

            // When & Then
            assertThatThrownBy(() -> datasetService.insertRecord(validRequest))
                    .isInstanceOf(DatasetValidationException.class);

            // Repository should never be called
            verify(datasetRepository, never()).save(any());
        }
    }

    // ==================== EDGE CASES ====================

    @Nested
    @DisplayName("Edge Cases")
    class EdgeCases {

        @Test
        @DisplayName("Should handle data with unicode characters")
        void testInsertRecord_UnicodeCharacters() {
            // Given: Data with unicode
            Map<String, Object> unicodeData = new HashMap<>();
            unicodeData.put("name", "José García");
            unicodeData.put("city", "北京");
            unicodeData.put("emoji", "😀🎉");

            InsertRecordRequest unicodeRequest = InsertRecordRequest.builder()
                    .datasetName("users")
                    .data(unicodeData)
                    .build();

            DatasetRecord unicodeRecord = DatasetRecord.builder()
                    .id(UUID.randomUUID())
                    .datasetName("users")
                    .data(unicodeData)
                    .createdAt(OffsetDateTime.now())
                    .updatedAt(OffsetDateTime.now())
                    .isDeleted(false)
                    .version(1)
                    .build();

            doNothing().when(jsonValidator).validateDatasetName(anyString());
            doNothing().when(jsonValidator).validateJsonData(any(), anyBoolean());
            when(datasetRepository.save(any(DatasetRecord.class))).thenReturn(unicodeRecord);

            // When & Then
            assertThatCode(() -> datasetService.insertRecord(unicodeRequest))
                    .doesNotThrowAnyException();
        }

        @Test
        @DisplayName("Should handle maximum dataset name length (255)")
        void testInsertRecord_MaxDatasetNameLength() {
            // Given: Name exactly 255 characters
            String maxLengthName = "a".repeat(255);
            InsertRecordRequest maxLengthRequest = InsertRecordRequest.builder()
                    .datasetName(maxLengthName)
                    .data(validData)
                    .build();

            DatasetRecord maxLengthRecord = DatasetRecord.builder()
                    .id(UUID.randomUUID())
                    .datasetName(maxLengthName)
                    .data(validData)
                    .createdAt(OffsetDateTime.now())
                    .updatedAt(OffsetDateTime.now())
                    .isDeleted(false)
                    .version(1)
                    .build();

            doNothing().when(jsonValidator).validateDatasetName(anyString());
            doNothing().when(jsonValidator).validateJsonData(any(), anyBoolean());
            when(datasetRepository.save(any(DatasetRecord.class))).thenReturn(maxLengthRecord);

            // When & Then
            assertThatCode(() -> datasetService.insertRecord(maxLengthRequest))
                    .doesNotThrowAnyException();
        }

        @Test
        @DisplayName("Should handle deeply nested JSON (10 levels)")
        void testInsertRecord_DeeplyNestedJson() {
            // Given: 10-level nested structure
            Map<String, Object> level10 = Map.of("value", "deepest");
            Map<String, Object> level9 = Map.of("level10", level10);
            Map<String, Object> level8 = Map.of("level9", level9);
            Map<String, Object> level7 = Map.of("level8", level8);
            Map<String, Object> level6 = Map.of("level7", level7);
            Map<String, Object> level5 = Map.of("level6", level6);
            Map<String, Object> level4 = Map.of("level5", level5);
            Map<String, Object> level3 = Map.of("level4", level4);
            Map<String, Object> level2 = Map.of("level3", level3);
            Map<String, Object> level1 = Map.of("level2", level2);

            InsertRecordRequest deeplyNestedRequest = InsertRecordRequest.builder()
                    .datasetName("users")
                    .data(level1)
                    .build();

            DatasetRecord deeplyNestedRecord = DatasetRecord.builder()
                    .id(UUID.randomUUID())
                    .datasetName("users")
                    .data(level1)
                    .createdAt(OffsetDateTime.now())
                    .updatedAt(OffsetDateTime.now())
                    .isDeleted(false)
                    .version(1)
                    .build();

            doNothing().when(jsonValidator).validateDatasetName(anyString());
            doNothing().when(jsonValidator).validateJsonData(any(), anyBoolean());
            when(datasetRepository.save(any(DatasetRecord.class))).thenReturn(deeplyNestedRecord);

            // When & Then
            assertThatCode(() -> datasetService.insertRecord(deeplyNestedRequest))
                    .doesNotThrowAnyException();
        }

        @Test
        @DisplayName("Should handle large array in JSON (1000 elements)")
        void testInsertRecord_LargeArray() {
            // Given: Array with 1000 elements
            List<Integer> largeArray = new ArrayList<>();
            for (int i = 0; i < 1000; i++) {
                largeArray.add(i);
            }

            Map<String, Object> largeArrayData = Map.of("numbers", largeArray);
            InsertRecordRequest largeArrayRequest = InsertRecordRequest.builder()
                    .datasetName("users")
                    .data(largeArrayData)
                    .build();

            DatasetRecord largeArrayRecord = DatasetRecord.builder()
                    .id(UUID.randomUUID())
                    .datasetName("users")
                    .data(largeArrayData)
                    .createdAt(OffsetDateTime.now())
                    .updatedAt(OffsetDateTime.now())
                    .isDeleted(false)
                    .version(1)
                    .build();

            doNothing().when(jsonValidator).validateDatasetName(anyString());
            doNothing().when(jsonValidator).validateJsonData(any(), anyBoolean());
            when(datasetRepository.save(any(DatasetRecord.class))).thenReturn(largeArrayRecord);

            // When & Then
            assertThatCode(() -> datasetService.insertRecord(largeArrayRequest))
                    .doesNotThrowAnyException();
        }
    }
}