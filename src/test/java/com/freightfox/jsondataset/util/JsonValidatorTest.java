package com.freightfox.jsondataset.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.freightfox.jsondataset.exception.InvalidJsonException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * Unit tests for JsonValidator.
 * 
 * TEST STRATEGY:
 * - Mock ObjectMapper to control serialization behavior
 * - Test validation rules in isolation
 * - Verify error messages are clear and actionable
 * 
 * COVERAGE TARGET: 90%+
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("JsonValidator Unit Tests")
class JsonValidatorTest {

    @Mock
    private ObjectMapper objectMapper;

    @InjectMocks
    private JsonValidator jsonValidator;

    private Map<String, Object> validData;

    @BeforeEach
    void setUp() {
        validData = new HashMap<>();
        validData.put("name", "John Doe");
        validData.put("age", 30);
        validData.put("active", true);
    }

    // ==================== VALIDATE JSON DATA ====================

    @Test
    @DisplayName("Should validate valid JSON data successfully")
    void testValidateJsonData_ValidData() throws Exception {
        // Given: Valid data and successful serialization
        when(objectMapper.writeValueAsString(validData))
                .thenReturn("{\"name\":\"John Doe\",\"age\":30,\"active\":true}");

        // When & Then: Should not throw exception
        assertThatCode(() -> jsonValidator.validateJsonData(validData))
                .doesNotThrowAnyException();

        verify(objectMapper).writeValueAsString(validData);
    }

    @Test
    @DisplayName("Should throw exception when data is null")
    void testValidateJsonData_NullData() {
        // When & Then: Should throw InvalidJsonException
        assertThatThrownBy(() -> jsonValidator.validateJsonData(null))
                .isInstanceOf(InvalidJsonException.class)
                .hasMessageContaining("JSON data cannot be null");

        // Verify ObjectMapper not called
        verifyNoInteractions(objectMapper);
    }

    @Test
    @DisplayName("Should throw exception when data is empty (strict mode)")
    void testValidateJsonData_EmptyData_StrictMode() {
        // Given: Empty map
        Map<String, Object> emptyData = new HashMap<>();

        // When & Then: Should throw exception in strict mode
        assertThatThrownBy(() -> jsonValidator.validateJsonData(emptyData, false))
                .isInstanceOf(InvalidJsonException.class)
                .hasMessageContaining("JSON data cannot be empty");
    }

    @Test
    @DisplayName("Should allow empty data in non-strict mode")
    void testValidateJsonData_EmptyData_NonStrictMode() throws Exception {
        // Given: Empty map
        Map<String, Object> emptyData = new HashMap<>();
        when(objectMapper.writeValueAsString(emptyData)).thenReturn("{}");

        // When & Then: Should not throw exception
        assertThatCode(() -> jsonValidator.validateJsonData(emptyData, true))
                .doesNotThrowAnyException();
    }

    @Test
    @DisplayName("Should throw exception when JSON contains null keys")
    void testValidateJsonData_NullKeys() {
        // Given: Data with null key
        Map<String, Object> dataWithNullKey = new HashMap<>();
        dataWithNullKey.put(null, "value");
        dataWithNullKey.put("name", "John");

        // When & Then: Should throw exception
        assertThatThrownBy(() -> jsonValidator.validateJsonData(dataWithNullKey))
                .isInstanceOf(InvalidJsonException.class)
                .hasMessageContaining("JSON keys cannot be null or blank");
    }

    @Test
    @DisplayName("Should throw exception when JSON contains blank keys")
    void testValidateJsonData_BlankKeys() {
        // Given: Data with blank key
        Map<String, Object> dataWithBlankKey = new HashMap<>();
        dataWithBlankKey.put("", "value");
        dataWithBlankKey.put("name", "John");

        // When & Then: Should throw exception
        assertThatThrownBy(() -> jsonValidator.validateJsonData(dataWithBlankKey))
                .isInstanceOf(InvalidJsonException.class)
                .hasMessageContaining("JSON keys cannot be null or blank");
    }

    @Test
    @DisplayName("Should throw exception when JSON is not serializable")
    void testValidateJsonData_NotSerializable() throws Exception {
        // Given: Serialization fails (circular reference, etc.)
        when(objectMapper.writeValueAsString(validData))
                .thenThrow(new RuntimeException("Circular reference detected"));

        // When & Then: Should throw InvalidJsonException with details
        assertThatThrownBy(() -> jsonValidator.validateJsonData(validData))
                .isInstanceOf(InvalidJsonException.class)
                .hasMessageContaining("JSON validation failed")
                .satisfies(ex -> {
                    InvalidJsonException jsonEx = (InvalidJsonException) ex;
                    assertThat(jsonEx.getValidationErrors())
                            .isNotNull()
                            .hasSize(1)
                            .anyMatch(msg -> msg.contains("not serializable"));
                });
    }

    // ==================== VALIDATE DATASET NAME ====================

    @Test
    @DisplayName("Should validate valid dataset name")
    void testValidateDatasetName_ValidName() {
        // When & Then: Valid names should not throw
        assertThatCode(() -> jsonValidator.validateDatasetName("users"))
                .doesNotThrowAnyException();
        
        assertThatCode(() -> jsonValidator.validateDatasetName("user_data"))
                .doesNotThrowAnyException();
        
        assertThatCode(() -> jsonValidator.validateDatasetName("users123"))
                .doesNotThrowAnyException();
        
        assertThatCode(() -> jsonValidator.validateDatasetName("user-data"))
                .doesNotThrowAnyException();
        
        assertThatCode(() -> jsonValidator.validateDatasetName("_private"))
                .doesNotThrowAnyException();
    }

    @Test
    @DisplayName("Should throw exception when dataset name is null")
    void testValidateDatasetName_Null() {
        // When & Then
        assertThatThrownBy(() -> jsonValidator.validateDatasetName(null))
                .isInstanceOf(InvalidJsonException.class)
                .hasMessageContaining("Dataset name cannot be null or blank");
    }

    @Test
    @DisplayName("Should throw exception when dataset name is blank")
    void testValidateDatasetName_Blank() {
        // When & Then
        assertThatThrownBy(() -> jsonValidator.validateDatasetName("   "))
                .isInstanceOf(InvalidJsonException.class)
                .hasMessageContaining("Dataset name cannot be null or blank");
    }

    @Test
    @DisplayName("Should throw exception when dataset name is too long")
    void testValidateDatasetName_TooLong() {
        // Given: Name > 255 characters
        String longName = "a".repeat(256);

        // When & Then
        assertThatThrownBy(() -> jsonValidator.validateDatasetName(longName))
                .isInstanceOf(InvalidJsonException.class)
                .hasMessageContaining("exceeds maximum length of 255 characters");
    }

    @Test
    @DisplayName("Should throw exception when dataset name has invalid characters")
    void testValidateDatasetName_InvalidCharacters() {
        // Test various invalid characters
        assertThatThrownBy(() -> jsonValidator.validateDatasetName("user data"))
                .isInstanceOf(InvalidJsonException.class)
                .hasMessageContaining("alphanumeric");

        assertThatThrownBy(() -> jsonValidator.validateDatasetName("user@data"))
                .isInstanceOf(InvalidJsonException.class);

        assertThatThrownBy(() -> jsonValidator.validateDatasetName("user!data"))
                .isInstanceOf(InvalidJsonException.class);

        assertThatThrownBy(() -> jsonValidator.validateDatasetName("user.data"))
                .isInstanceOf(InvalidJsonException.class);
    }

    @Test
    @DisplayName("Should throw exception when dataset name starts with number")
    void testValidateDatasetName_StartsWithNumber() {
        // When & Then
        assertThatThrownBy(() -> jsonValidator.validateDatasetName("123users"))
                .isInstanceOf(InvalidJsonException.class)
                .hasMessageContaining("must start with letter or underscore");
    }

    @Test
    @DisplayName("Should throw exception when dataset name starts with hyphen")
    void testValidateDatasetName_StartsWithHyphen() {
        // When & Then
        assertThatThrownBy(() -> jsonValidator.validateDatasetName("-users"))
                .isInstanceOf(InvalidJsonException.class)
                .hasMessageContaining("must start with letter or underscore");
    }

    // ==================== IS EFFECTIVELY EMPTY ====================

    @Test
    @DisplayName("Should detect null data as effectively empty")
    void testIsEffectivelyEmpty_NullData() {
        // When & Then
        assertThat(jsonValidator.isEffectivelyEmpty(null)).isTrue();
    }

    @Test
    @DisplayName("Should detect empty map as effectively empty")
    void testIsEffectivelyEmpty_EmptyMap() {
        // When & Then
        assertThat(jsonValidator.isEffectivelyEmpty(new HashMap<>())).isTrue();
    }

    @Test
    @DisplayName("Should detect map with only null values as effectively empty")
    void testIsEffectivelyEmpty_AllNullValues() {
        // Given: Map with null values
        Map<String, Object> nullValueMap = new HashMap<>();
        nullValueMap.put("key1", null);
        nullValueMap.put("key2", null);

        // When & Then
        assertThat(jsonValidator.isEffectivelyEmpty(nullValueMap)).isTrue();
    }

    @Test
    @DisplayName("Should detect map with valid values as not empty")
    void testIsEffectivelyEmpty_ValidValues() {
        // When & Then
        assertThat(jsonValidator.isEffectivelyEmpty(validData)).isFalse();
    }

    @Test
    @DisplayName("Should detect map with mixed null and valid values as not empty")
    void testIsEffectivelyEmpty_MixedValues() {
        // Given: Map with mixed values
        Map<String, Object> mixedMap = new HashMap<>();
        mixedMap.put("key1", null);
        mixedMap.put("key2", "value");

        // When & Then
        assertThat(jsonValidator.isEffectivelyEmpty(mixedMap)).isFalse();
    }
}