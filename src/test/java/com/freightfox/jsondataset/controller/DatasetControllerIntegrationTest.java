package com.freightfox.jsondataset.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.freightfox.jsondataset.dto.request.InsertRecordRequest;
import com.freightfox.jsondataset.model.entity.DatasetRecord;
import com.freightfox.jsondataset.repository.DatasetRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.transaction.annotation.Transactional;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultHandlers.print;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

/**

 * 
 * @SpringBootTest
 
 * 
 * @AutoConfigureMockMvc
 
 * @Transactional

 */
@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles("test")
@Transactional
@DisplayName("DatasetController Integration Tests - Insert Endpoint")
class DatasetControllerIntegrationTest {

    @Autowired
    private MockMvc mockMvc;

    
    @Autowired
    private ObjectMapper objectMapper;

   
    @Autowired
    private DatasetRepository datasetRepository;

    @BeforeEach
    void setUp() {
        datasetRepository.deleteAll();
    }

    @Test
    @DisplayName("Should insert record and return 201 Created")
    void testInsertRecord_Success() throws Exception {
        Map<String, Object> data = new HashMap<>();
        data.put("name", "John Doe");
        data.put("age", 30);
        data.put("email", "john@example.com");

        InsertRecordRequest request = InsertRecordRequest.builder()
                .datasetName("users")
                .data(data)
                .build();

        String requestJson = objectMapper.writeValueAsString(request);

        MvcResult result = mockMvc.perform(
                post("/api/datasets/users/record")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(requestJson)
        )
        .andDo(print())  
        .andExpect(status().isCreated())  
        .andExpect(content().contentType(MediaType.APPLICATION_JSON))
        .andExpect(jsonPath("$.success").value(true))
        .andExpect(jsonPath("$.message").value("Record inserted successfully"))
        .andExpect(jsonPath("$.record_id").exists())
        .andExpect(jsonPath("$.dataset_name").value("users"))
        .andExpect(jsonPath("$.created_at").exists())
        .andReturn();

        List<DatasetRecord> records = datasetRepository
                .findByDatasetNameAndIsDeleted("users", false);
        assertThat(records).hasSize(1);
        assertThat(records.get(0).getData()).containsEntry("name", "John Doe");
        assertThat(records.get(0).getData()).containsEntry("age", 30);

        String responseJson = result.getResponse().getContentAsString();
        assertThat(responseJson).contains("record_id");
    }

    @Test
    @DisplayName("Should handle empty data map")
    void testInsertRecord_EmptyData() throws Exception {
        InsertRecordRequest request = InsertRecordRequest.builder()
                .datasetName("users")
                .data(new HashMap<>())
                .build();

        String requestJson = objectMapper.writeValueAsString(request);

        // When & Then: Should accept empty map
        mockMvc.perform(
                post("/api/datasets/users/record")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(requestJson)
        )
        .andDo(print())
        .andExpect(status().isCreated())  // Empty map is allowed
        .andExpect(jsonPath("$.success").value(true));

        List<DatasetRecord> records = datasetRepository
                .findByDatasetNameAndIsDeleted("users", false);
        assertThat(records).hasSize(1);
        assertThat(records.get(0).getData()).isEmpty();
    }

    @Test
    @DisplayName("Should handle nested JSON data")
    void testInsertRecord_NestedData() throws Exception {
        // Given: Nested JSON structure
        Map<String, Object> address = new HashMap<>();
        address.put("city", "New York");
        address.put("zip", "10001");

        Map<String, Object> data = new HashMap<>();
        data.put("name", "John Doe");
        data.put("address", address);
        data.put("tags", List.of("developer", "java"));

        InsertRecordRequest request = InsertRecordRequest.builder()
                .datasetName("users")
                .data(data)
                .build();

        String requestJson = objectMapper.writeValueAsString(request);

        // When: Insert nested structure
        mockMvc.perform(
                post("/api/datasets/users/record")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(requestJson)
        )
        .andDo(print())
        .andExpect(status().isCreated())
        .andExpect(jsonPath("$.success").value(true));

        List<DatasetRecord> records = datasetRepository
                .findByDatasetNameAndIsDeleted("users", false);
        assertThat(records).hasSize(1);
        
        @SuppressWarnings("unchecked")
        Map<String, Object> savedAddress = 
                (Map<String, Object>) records.get(0).getData().get("address");
        assertThat(savedAddress.get("city")).isEqualTo("New York");
    }

    @Test
    @DisplayName("Should handle dataset name in path vs body mismatch")
    void testInsertRecord_DatasetNameMismatch() throws Exception {
        // Given: Path says "users", body says "products"
        Map<String, Object> data = Map.of("name", "Product A");
        
        InsertRecordRequest request = InsertRecordRequest.builder()
                .datasetName("products")  // Body says products
                .data(data)
                .build();

        String requestJson = objectMapper.writeValueAsString(request);

        mockMvc.perform(
                post("/api/datasets/users/record")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(requestJson)
        )
        .andDo(print())
        .andExpect(status().isCreated())
        .andExpect(jsonPath("$.dataset_name").value("users"));  // Path wins

        // Verify saved under "users" (path takes precedence)
        List<DatasetRecord> records = datasetRepository
                .findByDatasetNameAndIsDeleted("users", false);
        assertThat(records).hasSize(1);
    }

 

    @Test
    @DisplayName("Should return 400 when dataset name is blank")
    void testInsertRecord_BlankDatasetName() throws Exception {
        
        InsertRecordRequest request = InsertRecordRequest.builder()
                .datasetName("   ")  
                .data(Map.of("name", "John"))
                .build();

        String requestJson = objectMapper.writeValueAsString(request);

        mockMvc.perform(
                post("/api/datasets/   /record")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(requestJson)
        )
        .andDo(print())
        .andExpect(status().isBadRequest())  // 400
        .andExpect(jsonPath("$.status").value(400))
        .andExpect(jsonPath("$.code").value("VALIDATION_ERROR"))
        .andExpect(jsonPath("$.message").exists())
        .andExpect(jsonPath("$.details").isArray());
    }

    @Test
    @DisplayName("Should return 400 when data is null")
    void testInsertRecord_NullData() throws Exception {
        // Given: Null data (manual JSON to allow null)
        String requestJson = """
                {
                    "dataset_name": "users",
                    "data": null
                }
                """;

        // When & Then: Should return 400
        mockMvc.perform(
                post("/api/datasets/users/record")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(requestJson)
        )
        .andDo(print())
        .andExpect(status().isBadRequest())
        .andExpect(jsonPath("$.status").value(400))
        .andExpect(jsonPath("$.code").value("VALIDATION_ERROR"));
    }

    @Test
    @DisplayName("Should return 400 when dataset name has invalid characters")
    void testInsertRecord_InvalidDatasetName() throws Exception {
        // Given: Invalid dataset name (spaces, special chars)
        InsertRecordRequest request = InsertRecordRequest.builder()
                .datasetName("invalid dataset!")  // Invalid chars
                .data(Map.of("name", "John"))
                .build();

        String requestJson = objectMapper.writeValueAsString(request);

        // When & Then: Should return 400
        mockMvc.perform(
                post("/api/datasets/invalid%20dataset!/record")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(requestJson)
        )
        .andDo(print())
        .andExpect(status().isBadRequest())
        .andExpect(jsonPath("$.status").value(400))
        .andExpect(jsonPath("$.code").value("DATASET_VALIDATION_ERROR"));
    }

    // ==================== MALFORMED JSON TESTS ====================

    @Test
    @DisplayName("Should return 400 for malformed JSON")
    void testInsertRecord_MalformedJson() throws Exception {
        // Given: Invalid JSON syntax
        String malformedJson = """
                {
                    "dataset_name": "users",
                    "data": {invalid json}
                }
                """;

        // When & Then: Should return 400 with malformed JSON error
        mockMvc.perform(
                post("/api/datasets/users/record")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(malformedJson)
        )
        .andDo(print())
        .andExpect(status().isBadRequest())
        .andExpect(jsonPath("$.status").value(400))
        .andExpect(jsonPath("$.code").value("MALFORMED_JSON"));
    }

    @Test
    @DisplayName("Should return 400 for missing required fields")
    void testInsertRecord_MissingFields() throws Exception {
        // Given: Missing dataset_name field
        String incompleteJson = """
                {
                    "data": {"name": "John"}
                }
                """;

        // When & Then: Should return 400
        mockMvc.perform(
                post("/api/datasets/users/record")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(incompleteJson)
        )
        .andDo(print())
        .andExpect(status().isBadRequest())
        .andExpect(jsonPath("$.status").value(400))
        .andExpect(jsonPath("$.code").value("VALIDATION_ERROR"));
    }

    @Test
    @DisplayName("Should return 400 for empty request body")
    void testInsertRecord_EmptyBody() throws Exception {
        // When & Then: Empty body should fail
        mockMvc.perform(
                post("/api/datasets/users/record")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("")
        )
        .andDo(print())
        .andExpect(status().isBadRequest());
    }

    @Test
    @DisplayName("Should return 415 for unsupported media type")
    void testInsertRecord_UnsupportedMediaType() throws Exception {
        // Given: Wrong content type (text/plain instead of application/json)
        String requestJson = "{\"dataset_name\":\"users\",\"data\":{}}";

        // When & Then: Should return 415 Unsupported Media Type
        mockMvc.perform(
                post("/api/datasets/users/record")
                        .contentType(MediaType.TEXT_PLAIN)  // Wrong type!
                        .content(requestJson)
        )
        .andDo(print())
        .andExpect(status().isUnsupportedMediaType());  // 415
    }

    

    @Test
    @DisplayName("Should return health status")
    void testHealthEndpoint() throws Exception {
        
        mockMvc.perform(get("/api/datasets/health"))
                .andDo(print())
                .andExpect(status().isOk())  // 200
                .andExpect(jsonPath("$.status").value("UP"))
                .andExpect(jsonPath("$.message").value("Dataset API is running"));
    }

    

    @Test
    @DisplayName("Should handle multiple inserts efficiently")
    void testInsertRecord_MultipleInserts() throws Exception {
        
        for (int i = 0; i < 10; i++) {
            Map<String, Object> data = Map.of(
                    "name", "User " + i,
                    "index", i
            );

            InsertRecordRequest request = InsertRecordRequest.builder()
                    .datasetName("users")
                    .data(data)
                    .build();

            String requestJson = objectMapper.writeValueAsString(request);

            // When: Insert each record
            mockMvc.perform(
                    post("/api/datasets/users/record")
                            .contentType(MediaType.APPLICATION_JSON)
                            .content(requestJson)
            )
            .andExpect(status().isCreated());
        }

        // Then: Verify all 10 records saved
        List<DatasetRecord> records = datasetRepository
                .findByDatasetNameAndIsDeleted("users", false);
        assertThat(records).hasSize(10);
    }
}
