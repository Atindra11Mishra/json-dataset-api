package com.freightfox.jsondataset.exception;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.freightfox.jsondataset.dto.request.InsertRecordRequest;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.servlet.MockMvc;

import java.util.HashMap;
import java.util.Map;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultHandlers.print;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;


@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles("test")
@DisplayName("Global Exception Handler Tests")
class GlobalExceptionHandlerTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    

    @Test
    @DisplayName("Should return 400 with details when @Valid fails")
    void testValidationError_NotBlank() throws Exception {
        // Given: Request with blank dataset name
        InsertRecordRequest request = InsertRecordRequest.builder()
                .datasetName("")  // Blank - violates @NotBlank
                .data(Map.of("name", "John"))
                .build();

        // When & Then
        mockMvc.perform(
                post("/api/datasets/test/record")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request))
        )
        .andDo(print())
        .andExpect(status().isBadRequest())  // 400
        .andExpect(jsonPath("$.timestamp").exists())
        .andExpect(jsonPath("$.status").value(400))
        .andExpect(jsonPath("$.error").value("Bad Request"))
        .andExpect(jsonPath("$.message").value("Request validation failed"))
        .andExpect(jsonPath("$.code").value("VALIDATION_ERROR"))
        .andExpect(jsonPath("$.details").isArray())
        .andExpect(jsonPath("$.details[0]").value("datasetName: Dataset name is required and cannot be blank"))
        .andExpect(jsonPath("$.path").value("/api/datasets/test/record"));
    }

    @Test
    @DisplayName("Should return 400 when data is null")
    void testValidationError_NotNull() throws Exception {
        // Given: Request with null data
        String requestJson = """
                {
                    "dataset_name": "users",
                    "data": null
                }
                """;

        // When & Then
        mockMvc.perform(
                post("/api/datasets/users/record")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(requestJson)
        )
        .andDo(print())
        .andExpect(status().isBadRequest())
        .andExpect(jsonPath("$.status").value(400))
        .andExpect(jsonPath("$.error").value("Bad Request"))
        .andExpect(jsonPath("$.code").value("VALIDATION_ERROR"))
        .andExpect(jsonPath("$.details[0]").value("data: Data is required and cannot be null"))
        .andExpect(jsonPath("$.path").exists());
    }

    

    @Test
    @DisplayName("Should return 400 for malformed JSON")
    void testMalformedJson() throws Exception {
        // Given: Invalid JSON syntax
        String malformedJson = """
                {
                    "dataset_name": "users",
                    "data": {invalid json}
                }
                """;

        // When & Then
        mockMvc.perform(
                post("/api/datasets/users/record")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(malformedJson)
        )
        .andDo(print())
        .andExpect(status().isBadRequest())
        .andExpect(jsonPath("$.status").value(400))
        .andExpect(jsonPath("$.error").value("Bad Request"))
        .andExpect(jsonPath("$.message").value("Malformed JSON in request body"))
        .andExpect(jsonPath("$.code").value("MALFORMED_JSON"))
        .andExpect(jsonPath("$.details").isArray())
        .andExpect(jsonPath("$.path").value("/api/datasets/users/record"));
    }

   

    @Test
    @DisplayName("Should return 400 for invalid dataset name")
    void testInvalidDatasetName() throws Exception {
        // Given: Dataset name with invalid characters
        InsertRecordRequest request = InsertRecordRequest.builder()
                .datasetName("invalid name!")  // Space and special char
                .data(Map.of("name", "John"))
                .build();

        // When & Then
        mockMvc.perform(
                post("/api/datasets/invalid%20name!/record")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request))
        )
        .andDo(print())
        .andExpect(status().isBadRequest())
        .andExpect(jsonPath("$.status").value(400))
        .andExpect(jsonPath("$.error").value("Bad Request"))
        .andExpect(jsonPath("$.code").value("DATASET_VALIDATION_ERROR"))
        .andExpect(jsonPath("$.details").isArray());
    }

    

    @Test
    @DisplayName("Should return 400 when neither groupBy nor sortBy provided")
    void testQueryError_NoParameters() throws Exception {
        // When: No query params
        mockMvc.perform(
                get("/api/datasets/users/query")
        )
        .andDo(print())
        .andExpect(status().isBadRequest())
        .andExpect(jsonPath("$.status").value(400))
        .andExpect(jsonPath("$.message").value("At least one query parameter (groupBy or sortBy) must be provided"))
        .andExpect(jsonPath("$.path").value("/api/datasets/users/query"));
    }

    @Test
    @DisplayName("Should return 400 for invalid sort order")
    void testInvalidSortOrder() throws Exception {
        // When: Invalid order value
        mockMvc.perform(
                get("/api/datasets/users/query")
                        .param("sortBy", "age")
                        .param("order", "invalid")
        )
        .andDo(print())
        .andExpect(status().isBadRequest())
        .andExpect(jsonPath("$.status").value(400))
        .andExpect(jsonPath("$.code").value("INVALID_SORT_ORDER"))
        .andExpect(jsonPath("$.details[0]").value("Valid sort orders: 'asc' (ascending) or 'desc' (descending)"));
    }

    

    @Test
    @DisplayName("Should return 405 for wrong HTTP method")
    void testMethodNotAllowed() throws Exception {
        // When: GET to POST endpoint
        mockMvc.perform(
                get("/api/datasets/users/record")  // Should be POST
        )
        .andDo(print())
        .andExpect(status().isMethodNotAllowed())  // 405
        .andExpect(jsonPath("$.status").value(405))
        .andExpect(jsonPath("$.error").value("Method Not Allowed"))
        .andExpect(jsonPath("$.code").value("METHOD_NOT_ALLOWED"))
        .andExpect(jsonPath("$.details[0]").value("Supported methods: POST"));
    }

    

    @Test
    @DisplayName("Should return 415 for wrong Content-Type")
    void testUnsupportedMediaType() throws Exception {
        // When: text/plain instead of application/json
        mockMvc.perform(
                post("/api/datasets/users/record")
                        .contentType(MediaType.TEXT_PLAIN)
                        .content("{\"dataset_name\":\"users\"}")
        )
        .andDo(print())
        .andExpect(status().isUnsupportedMediaType())  // 415
        .andExpect(jsonPath("$.status").value(415))
        .andExpect(jsonPath("$.error").value("Unsupported Media Type"))
        .andExpect(jsonPath("$.code").value("UNSUPPORTED_MEDIA_TYPE"))
        .andExpect(jsonPath("$.details[0]").exists());
    }

    

    @Test
    @DisplayName("Should return 404 for non-existent endpoint")
    void testEndpointNotFound() throws Exception {
        // When: Request to non-existent endpoint
        mockMvc.perform(
                get("/api/datasets/users/nonexistent")
        )
        .andDo(print())
        .andExpect(status().isNotFound())  // 404
        .andExpect(jsonPath("$.status").value(404))
        .andExpect(jsonPath("$.error").value("Not Found"))
        .andExpect(jsonPath("$.message").value("The requested endpoint does not exist"))
        .andExpect(jsonPath("$.code").value("ENDPOINT_NOT_FOUND"));
    }

    

    @Test
    @DisplayName("Should have consistent error response format")
    void testErrorResponseFormat() throws Exception {
        // When: Trigger any error
        mockMvc.perform(
                post("/api/datasets/users/record")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{malformed}")
        )
        .andDo(print())
        .andExpect(status().isBadRequest())
        // Verify all required fields present
        .andExpect(jsonPath("$.timestamp").exists())
        .andExpect(jsonPath("$.status").exists())
        .andExpect(jsonPath("$.error").exists())
        .andExpect(jsonPath("$.message").exists())
        .andExpect(jsonPath("$.path").exists())
        // Verify timestamp format (ISO-8601)
        .andExpect(jsonPath("$.timestamp").value(org.hamcrest.Matchers.matchesRegex(
                "\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}")));
    }

    

    @Test
    @DisplayName("Should provide helpful error messages")
    void testUserFriendlyMessages() throws Exception {
        // Scenario 1: Missing required field
        mockMvc.perform(
                post("/api/datasets/users/record")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"data\": {\"name\": \"John\"}}")  // Missing dataset_name
        )
        .andDo(print())
        .andExpect(jsonPath("$.message").value("Request validation failed"))
        .andExpect(jsonPath("$.details").isArray());

        
        InsertRecordRequest request = InsertRecordRequest.builder()
                .datasetName("test@dataset")
                .data(Map.of("name", "John"))
                .build();

        mockMvc.perform(
                post("/api/datasets/test@dataset/record")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request))
        )
        .andDo(print())
        .andExpect(jsonPath("$.message").exists())
        .andExpect(jsonPath("$.details").isArray());
    }
}