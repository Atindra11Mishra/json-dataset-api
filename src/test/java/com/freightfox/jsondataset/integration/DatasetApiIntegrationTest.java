package com.freightfox.jsondataset.integration;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.freightfox.jsondataset.dto.request.InsertRecordRequest;
import com.freightfox.jsondataset.model.entity.DatasetRecord;
import com.freightfox.jsondataset.repository.DatasetRepository;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;
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
 * n
 * 
 * @AutoConfigureMockMvc
 * 
 * 
 * @Transactional (on class or method)
 * -
 * @TestPropertySource

 * 
 * @TestMethodOrder
 
 */
@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles("test")
@Transactional  // Each test runs in transaction and rolls back
@TestPropertySource(properties = {
        "spring.jpa.hibernate.ddl-auto=create-drop",
        "logging.level.org.hibernate.SQL=DEBUG"
})
@DisplayName("Dataset API Integration Tests")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class DatasetApiIntegrationTest {

    /**
    
     */
    @Autowired
    private MockMvc mockMvc;

    /**
    
     */
    @Autowired
    private ObjectMapper objectMapper;

    /**
     
     */
    @Autowired
    private DatasetRepository datasetRepository;

    /**
     * Test data holders
     */
    private static final String DATASET_NAME = "users";
    private Map<String, Object> sampleData;

    /**
     * Setup before each test.
     * 
     * @BeforeEach runs before every test method.
     * 
     *
     */
    @BeforeEach
    void setUp() {
        // Clean database (redundant with @Transactional but explicit)
        datasetRepository.deleteAll();

        // Initialize sample data
        sampleData = new HashMap<>();
        sampleData.put("name", "John Doe");
        sampleData.put("age", 30);
        sampleData.put("email", "john@example.com");
        sampleData.put("status", "active");
    }

    /**
     * Cleanup after each test.
     * 
     * @AfterEach runs after every test method.
     * 
     *
     */
    @AfterEach
    void tearDown() {
        // Clean up test data
        // With @Transactional, this is automatic via rollback
        datasetRepository.deleteAll();
    }

    /**
    
     * 
     *
     */
    private String insertRecord(String datasetName, Map<String, Object> data) throws Exception {
        InsertRecordRequest request = InsertRecordRequest.builder()
                .datasetName(datasetName)
                .data(data)
                .build();

        MvcResult result = mockMvc.perform(
                post("/api/datasets/{datasetName}/record", datasetName)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request))
        )
        .andExpect(status().isCreated())
        .andExpect(jsonPath("$.success").value(true))
        .andExpect(jsonPath("$.record_id").exists())
        .andReturn();

        // Extract and return record ID
        String responseJson = result.getResponse().getContentAsString();
        Map<String, Object> response = objectMapper.readValue(responseJson, Map.class);
        return (String) response.get("record_id");
    }

    

    @Nested
    @DisplayName("Insert Record Integration Tests")
    class InsertRecordTests {

        @Test
        @Order(1)
        @DisplayName("Should insert record and persist to database")
        void testInsertRecord_DatabasePersistence() throws Exception {
            // Given: Valid insert request
            InsertRecordRequest request = InsertRecordRequest.builder()
                    .datasetName(DATASET_NAME)
                    .data(sampleData)
                    .build();

            // When: POST to insert endpoint
            MvcResult result = mockMvc.perform(
                    post("/api/datasets/{datasetName}/record", DATASET_NAME)
                            .contentType(MediaType.APPLICATION_JSON)
                            .content(objectMapper.writeValueAsString(request))
            )
            .andDo(print())
            .andExpect(status().isCreated())  // 201
            .andExpect(jsonPath("$.success").value(true))
            .andExpect(jsonPath("$.message").value("Record inserted successfully"))
            .andExpect(jsonPath("$.record_id").exists())
            .andExpect(jsonPath("$.dataset_name").value(DATASET_NAME))
            .andExpect(jsonPath("$.created_at").exists())
            .andReturn();

            // Then: Verify database persistence
            List<DatasetRecord> records = datasetRepository
                    .findByDatasetNameAndIsDeleted(DATASET_NAME, false);

            assertThat(records).hasSize(1);
            
            DatasetRecord savedRecord = records.get(0);
            assertThat(savedRecord.getId()).isNotNull();
            assertThat(savedRecord.getDatasetName()).isEqualTo(DATASET_NAME);
            assertThat(savedRecord.getData()).containsEntry("name", "John Doe");
            assertThat(savedRecord.getData()).containsEntry("age", 30);
            assertThat(savedRecord.getCreatedAt()).isNotNull();
            assertThat(savedRecord.getUpdatedAt()).isNotNull();
            assertThat(savedRecord.getIsDeleted()).isFalse();
            assertThat(savedRecord.getVersion()).isEqualTo(1);

            // Verify response contains correct ID
            String responseJson = result.getResponse().getContentAsString();
            Map<String, Object> response = objectMapper.readValue(responseJson, Map.class);
            String returnedId = (String) response.get("record_id");
            assertThat(savedRecord.getId().toString()).isEqualTo(returnedId);
        }

        @Test
        @DisplayName("Should handle multiple inserts to same dataset")
        void testInsertRecord_MultipleInserts() throws Exception {
            // Given: Multiple records for same dataset
            Map<String, Object> data1 = Map.of("name", "John", "age", 30);
            Map<String, Object> data2 = Map.of("name", "Jane", "age", 25);
            Map<String, Object> data3 = Map.of("name", "Bob", "age", 45);

            // When: Insert all three
            insertRecord(DATASET_NAME, data1);
            insertRecord(DATASET_NAME, data2);
            insertRecord(DATASET_NAME, data3);

            // Then: Verify all saved
            List<DatasetRecord> records = datasetRepository
                    .findByDatasetNameAndIsDeleted(DATASET_NAME, false);

            assertThat(records).hasSize(3);
            assertThat(records)
                    .extracting(record -> record.getData().get("name"))
                    .containsExactlyInAnyOrder("John", "Jane", "Bob");
        }

        @Test
        @DisplayName("Should handle nested JSON in insert")
        void testInsertRecord_NestedJson() throws Exception {
            // Given: Nested JSON structure
            Map<String, Object> address = new HashMap<>();
            address.put("city", "New York");
            address.put("zip", "10001");
            address.put("country", "USA");

            Map<String, Object> nestedData = new HashMap<>();
            nestedData.put("name", "John Doe");
            nestedData.put("address", address);
            nestedData.put("tags", List.of("developer", "java", "spring"));

            // When: Insert nested data
            String recordId = insertRecord(DATASET_NAME, nestedData);

            // Then: Verify nested structure preserved
            List<DatasetRecord> records = datasetRepository
                    .findByDatasetNameAndIsDeleted(DATASET_NAME, false);

            assertThat(records).hasSize(1);
            Map<String, Object> savedData = records.get(0).getData();

            assertThat(savedData.get("name")).isEqualTo("John Doe");
            
            @SuppressWarnings("unchecked")
            Map<String, Object> savedAddress = (Map<String, Object>) savedData.get("address");
            assertThat(savedAddress).containsEntry("city", "New York");
            assertThat(savedAddress).containsEntry("zip", "10001");

            @SuppressWarnings("unchecked")
            List<String> savedTags = (List<String>) savedData.get("tags");
            assertThat(savedTags).containsExactly("developer", "java", "spring");
        }

        @Test
        @DisplayName("Should return 400 for invalid insert request")
        void testInsertRecord_InvalidRequest() throws Exception {
            // Given: Invalid request (null data)
            String invalidJson = """
                    {
                        "dataset_name": "users",
                        "data": null
                    }
                    """;

            // When & Then: Should return 400
            mockMvc.perform(
                    post("/api/datasets/{datasetName}/record", DATASET_NAME)
                            .contentType(MediaType.APPLICATION_JSON)
                            .content(invalidJson)
            )
            .andDo(print())
            .andExpect(status().isBadRequest())  // 400
            .andExpect(jsonPath("$.timestamp").exists())
            .andExpect(jsonPath("$.status").value(400))
            .andExpect(jsonPath("$.error").value("Bad Request"))
            .andExpect(jsonPath("$.path").exists());

            // Verify nothing saved to database
            List<DatasetRecord> records = datasetRepository.findAll();
            assertThat(records).isEmpty();
        }
    }

    

    @Nested
    @DisplayName("Group-By Integration Tests")
    class GroupByTests {

        @Test
        @DisplayName("Should group records by field after insert")
        void testGroupBy_AfterInsert() throws Exception {
            // Given: Insert multiple records with different statuses
            insertRecord(DATASET_NAME, Map.of("name", "John", "age", 30, "status", "active"));
            insertRecord(DATASET_NAME, Map.of("name", "Jane", "age", 25, "status", "active"));
            insertRecord(DATASET_NAME, Map.of("name", "Bob", "age", 45, "status", "inactive"));
            insertRecord(DATASET_NAME, Map.of("name", "Alice", "age", 35, "status", "pending"));

            // When: Query with groupBy=status
            MvcResult result = mockMvc.perform(
                    get("/api/datasets/{datasetName}/query", DATASET_NAME)
                            .param("groupBy", "status")
            )
            .andDo(print())
            .andExpect(status().isOk())  // 200
            .andExpect(jsonPath("$.success").value(true))
            .andExpect(jsonPath("$.operation").value("group_by"))
            .andExpect(jsonPath("$.field").value("status"))
            .andExpect(jsonPath("$.total_records").value(4))
            .andExpect(jsonPath("$.groups").exists())
            .andReturn();

            // Then: Verify grouping
            String responseJson = result.getResponse().getContentAsString();
            Map<String, Object> response = objectMapper.readValue(responseJson, Map.class);
            
            @SuppressWarnings("unchecked")
            Map<String, List<Map<String, Object>>> groups = 
                    (Map<String, List<Map<String, Object>>>) response.get("groups");

            // Verify group structure
            assertThat(groups).containsKeys("active", "inactive", "pending");
            assertThat(groups.get("active")).hasSize(2);
            assertThat(groups.get("inactive")).hasSize(1);
            assertThat(groups.get("pending")).hasSize(1);

            // Verify group contents
            assertThat(groups.get("active"))
                    .extracting(data -> data.get("name"))
                    .containsExactlyInAnyOrder("John", "Jane");
            
            assertThat(groups.get("inactive").get(0).get("name")).isEqualTo("Bob");
            assertThat(groups.get("pending").get(0).get("name")).isEqualTo("Alice");

            // Verify metadata
            @SuppressWarnings("unchecked")
            Map<String, Object> metadata = (Map<String, Object>) response.get("metadata");
            assertThat(metadata.get("total_groups")).isEqualTo(3);
            assertThat(metadata.get("records_with_missing_field")).isEqualTo(0);
        }

        @Test
        @DisplayName("Should handle groupBy on missing field")
        void testGroupBy_MissingField() throws Exception {
            // Given: Records without "department" field
            insertRecord(DATASET_NAME, Map.of("name", "John", "age", 30));
            insertRecord(DATASET_NAME, Map.of("name", "Jane", "age", 25));

            // When: Group by non-existent field
            mockMvc.perform(
                    get("/api/datasets/{datasetName}/query", DATASET_NAME)
                            .param("groupBy", "department")
            )
            .andDo(print())
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.groups.__missing__").exists())
            .andExpect(jsonPath("$.groups.__missing__.length()").value(2))
            .andExpect(jsonPath("$.metadata.records_with_missing_field").value(2));
        }

        @Test
        @DisplayName("Should return empty result for non-existent dataset")
        void testGroupBy_NonExistentDataset() throws Exception {
            // When: Query non-existent dataset
            mockMvc.perform(
                    get("/api/datasets/{datasetName}/query", "nonexistent")
                            .param("groupBy", "status")
            )
            .andDo(print())
            .andExpect(status().isOk())  // 200 with empty result
            .andExpect(jsonPath("$.success").value(true))
            .andExpect(jsonPath("$.total_records").value(0))
            .andExpect(jsonPath("$.groups").isEmpty());
        }
    }

   

    @Nested
    @DisplayName("Sort-By Integration Tests")
    class SortByTests {

        @Test
        @DisplayName("Should sort records by numeric field ascending")
        void testSortBy_NumericAscending() throws Exception {
            // Given: Insert records with different ages
            insertRecord(DATASET_NAME, Map.of("name", "John", "age", 30));
            insertRecord(DATASET_NAME, Map.of("name", "Jane", "age", 25));
            insertRecord(DATASET_NAME, Map.of("name", "Bob", "age", 45));
            insertRecord(DATASET_NAME, Map.of("name", "Alice", "age", 35));

            // When: Sort by age ascending
            MvcResult result = mockMvc.perform(
                    get("/api/datasets/{datasetName}/query", DATASET_NAME)
                            .param("sortBy", "age")
                            .param("order", "asc")
            )
            .andDo(print())
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.operation").value("sort_by"))
            .andExpect(jsonPath("$.field").value("age"))
            .andExpect(jsonPath("$.results").isArray())
            .andExpect(jsonPath("$.results.length()").value(4))
            .andReturn();

            // Then: Verify order (25, 30, 35, 45)
            String responseJson = result.getResponse().getContentAsString();
            Map<String, Object> response = objectMapper.readValue(responseJson, Map.class);
            
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> results = (List<Map<String, Object>>) response.get("results");

            assertThat(results.get(0).get("age")).isEqualTo(25);
            assertThat(results.get(0).get("name")).isEqualTo("Jane");
            
            assertThat(results.get(1).get("age")).isEqualTo(30);
            assertThat(results.get(1).get("name")).isEqualTo("John");
            
            assertThat(results.get(2).get("age")).isEqualTo(35);
            assertThat(results.get(2).get("name")).isEqualTo("Alice");
            
            assertThat(results.get(3).get("age")).isEqualTo(45);
            assertThat(results.get(3).get("name")).isEqualTo("Bob");

            // Verify sort metadata
            @SuppressWarnings("unchecked")
            Map<String, Object> sortMetadata = (Map<String, Object>) response.get("sort_metadata");
            assertThat(sortMetadata.get("sort_order")).isEqualTo("asc");
            assertThat(sortMetadata.get("field_type")).isEqualTo("numeric");
        }

        @Test
        @DisplayName("Should sort records by string field descending")
        void testSortBy_StringDescending() throws Exception {
            // Given: Insert records with different names
            insertRecord(DATASET_NAME, Map.of("name", "Charlie", "age", 30));
            insertRecord(DATASET_NAME, Map.of("name", "Alice", "age", 25));
            insertRecord(DATASET_NAME, Map.of("name", "Diana", "age", 35));
            insertRecord(DATASET_NAME, Map.of("name", "Bob", "age", 40));

            // When: Sort by name descending
            MvcResult result = mockMvc.perform(
                    get("/api/datasets/{datasetName}/query", DATASET_NAME)
                            .param("sortBy", "name")
                            .param("order", "desc")
            )
            .andDo(print())
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.sort_metadata.sort_order").value("desc"))
            .andExpect(jsonPath("$.sort_metadata.field_type").value("string"))
            .andReturn();

            // Then: Verify reverse alphabetical order
            String responseJson = result.getResponse().getContentAsString();
            Map<String, Object> response = objectMapper.readValue(responseJson, Map.class);
            
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> results = (List<Map<String, Object>>) response.get("results");

            assertThat(results.get(0).get("name")).isEqualTo("Diana");
            assertThat(results.get(1).get("name")).isEqualTo("Charlie");
            assertThat(results.get(2).get("name")).isEqualTo("Bob");
            assertThat(results.get(3).get("name")).isEqualTo("Alice");
        }

        @Test
        @DisplayName("Should handle null values in sorting")
        void testSortBy_NullValues() throws Exception {
            // Given: Records with some null ages
            insertRecord(DATASET_NAME, Map.of("name", "John", "age", 30));
            insertRecord(DATASET_NAME, Map.of("name", "Jane", "age", 25));
            
            // Record with null age
            Map<String, Object> nullAgeData = new HashMap<>();
            nullAgeData.put("name", "Bob");
            nullAgeData.put("age", null);
            insertRecord(DATASET_NAME, nullAgeData);
            
            insertRecord(DATASET_NAME, Map.of("name", "Alice", "age", 35));

            // When: Sort by age
            MvcResult result = mockMvc.perform(
                    get("/api/datasets/{datasetName}/query", DATASET_NAME)
                            .param("sortBy", "age")
                            .param("order", "asc")
            )
            .andDo(print())
            .andExpect(status().isOk())
            .andReturn();

            // Then: Nulls should be at end (25, 30, 35, null)
            String responseJson = result.getResponse().getContentAsString();
            Map<String, Object> response = objectMapper.readValue(responseJson, Map.class);
            
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> results = (List<Map<String, Object>>) response.get("results");

            // First three have values
            assertThat(results.get(0).get("age")).isEqualTo(25);
            assertThat(results.get(1).get("age")).isEqualTo(30);
            assertThat(results.get(2).get("age")).isEqualTo(35);
            
            // Last one has null (sorted to end)
            assertThat(results.get(3).get("age")).isNull();
            assertThat(results.get(3).get("name")).isEqualTo("Bob");

            // Nulls may be represented as missing fields by serializer configuration.
            @SuppressWarnings("unchecked")
            Map<String, Object> sortMetadata = (Map<String, Object>) response.get("sort_metadata");
            Number nullCount = (Number) sortMetadata.get("records_with_null_field");
            Number missingCount = (Number) sortMetadata.get("records_with_missing_field");
            assertThat(nullCount.intValue() + missingCount.intValue()).isEqualTo(1);
        }

        @Test
        @DisplayName("Should return 400 for invalid sort order")
        void testSortBy_InvalidSortOrder() throws Exception {
            // Given: Some records
            insertRecord(DATASET_NAME, Map.of("name", "John", "age", 30));

            // When: Invalid sort order
            mockMvc.perform(
                    get("/api/datasets/{datasetName}/query", DATASET_NAME)
                            .param("sortBy", "age")
                            .param("order", "invalid")
            )
            .andDo(print())
            .andExpect(status().isBadRequest())  // 400
            .andExpect(jsonPath("$.status").value(400))
            .andExpect(jsonPath("$.code").value("INVALID_SORT_ORDER"))
            .andExpect(jsonPath("$.message").exists());
        }
    }

    

    @Nested
    @DisplayName("Group-By-Then-Sort Integration Tests")
    class GroupByThenSortTests {

        @Test
        @DisplayName("Should group then sort within groups")
        void testGroupByThenSort_Complete() throws Exception {
            // Given: Insert records with status and age
            insertRecord(DATASET_NAME, Map.of("name", "John", "age", 30, "status", "active"));
            insertRecord(DATASET_NAME, Map.of("name", "Jane", "age", 25, "status", "active"));
            insertRecord(DATASET_NAME, Map.of("name", "Charlie", "age", 28, "status", "active"));
            insertRecord(DATASET_NAME, Map.of("name", "Bob", "age", 45, "status", "inactive"));
            insertRecord(DATASET_NAME, Map.of("name", "Diana", "age", 40, "status", "inactive"));

            // When: Group by status, sort by age within groups
            MvcResult result = mockMvc.perform(
                    get("/api/datasets/{datasetName}/query", DATASET_NAME)
                            .param("groupBy", "status")
                            .param("sortBy", "age")
                            .param("order", "asc")
            )
            .andDo(print())
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.operation").value("group_by_then_sort"))
            .andExpect(jsonPath("$.field").value("status"))
            .andReturn();

            // Then: Verify grouping and sorting
            String responseJson = result.getResponse().getContentAsString();
            Map<String, Object> response = objectMapper.readValue(responseJson, Map.class);
            
            @SuppressWarnings("unchecked")
            Map<String, List<Map<String, Object>>> groups = 
                    (Map<String, List<Map<String, Object>>>) response.get("groups");

            // Verify active group sorted by age (25, 28, 30)
            List<Map<String, Object>> activeGroup = groups.get("active");
            assertThat(activeGroup).hasSize(3);
            assertThat(activeGroup.get(0).get("age")).isEqualTo(25);  // Jane
            assertThat(activeGroup.get(1).get("age")).isEqualTo(28);  // Charlie
            assertThat(activeGroup.get(2).get("age")).isEqualTo(30);  // John

            // Verify inactive group sorted by age (40, 45)
            List<Map<String, Object>> inactiveGroup = groups.get("inactive");
            assertThat(inactiveGroup).hasSize(2);
            assertThat(inactiveGroup.get(0).get("age")).isEqualTo(40);  // Diana
            assertThat(inactiveGroup.get(1).get("age")).isEqualTo(45);  // Bob
        }
    }

    

    @Nested
    @DisplayName("Error Handling Integration Tests")
    class ErrorHandlingTests {

        @Test
        @DisplayName("Should return 400 for missing query parameters")
        void testQuery_MissingParameters() throws Exception {
            // Given: Some records
            insertRecord(DATASET_NAME, Map.of("name", "John"));

            // When: Query without groupBy or sortBy
            mockMvc.perform(
                    get("/api/datasets/{datasetName}/query", DATASET_NAME)
            )
            .andDo(print())
            .andExpect(status().isBadRequest())
            .andExpect(jsonPath("$.status").value(400))
            .andExpect(jsonPath("$.error").value("Bad Request"))
            .andExpect(jsonPath("$.message").value("At least one query parameter (groupBy or sortBy) must be provided"))
            .andExpect(jsonPath("$.timestamp").exists())
            .andExpect(jsonPath("$.path").value("/api/datasets/users/query"));
        }

        @Test
        @DisplayName("Should return 405 for wrong HTTP method")
        void testWrongHttpMethod() throws Exception {
            // When: GET to POST endpoint
            mockMvc.perform(
                    get("/api/datasets/{datasetName}/record", DATASET_NAME)
            )
            .andDo(print())
            .andExpect(status().isMethodNotAllowed())  // 405
            .andExpect(jsonPath("$.status").value(405))
            .andExpect(jsonPath("$.error").value("Method Not Allowed"))
            .andExpect(jsonPath("$.code").value("METHOD_NOT_ALLOWED"));
        }

        @Test
        @DisplayName("Should return 415 for wrong Content-Type")
        void testUnsupportedMediaType() throws Exception {
            // When: text/plain instead of application/json
            mockMvc.perform(
                    post("/api/datasets/{datasetName}/record", DATASET_NAME)
                            .contentType(MediaType.TEXT_PLAIN)
                            .content("not json")
            )
            .andDo(print())
            .andExpect(status().isUnsupportedMediaType())  // 415
            .andExpect(jsonPath("$.status").value(415))
            .andExpect(jsonPath("$.code").value("UNSUPPORTED_MEDIA_TYPE"));
        }

        @Test
        @DisplayName("Should return 400 for malformed JSON")
        void testMalformedJson() throws Exception {
            // When: Invalid JSON syntax
            mockMvc.perform(
                    post("/api/datasets/{datasetName}/record", DATASET_NAME)
                            .contentType(MediaType.APPLICATION_JSON)
                            .content("{invalid json}")
            )
            .andDo(print())
            .andExpect(status().isBadRequest())
            .andExpect(jsonPath("$.status").value(400))
            .andExpect(jsonPath("$.code").value("MALFORMED_JSON"))
            .andExpect(jsonPath("$.message").value("Malformed JSON in request body"));
        }
    }

    

    @Nested
    @DisplayName("Transaction Management Tests")
    class TransactionTests {

        @Test
        @DisplayName("Should rollback transaction after test")
        void testTransactionRollback() throws Exception {
            // Given: Insert a record in this test
            insertRecord(DATASET_NAME, Map.of("name", "Test User", "age", 25));

            // Verify it exists within this test
            List<DatasetRecord> records = datasetRepository
                    .findByDatasetNameAndIsDeleted(DATASET_NAME, false);
            assertThat(records).hasSize(1);

        }

        @Test
        @DisplayName("Should start with clean database (previous test rolled back)")
        void testCleanDatabase() {
           
            
            List<DatasetRecord> records = datasetRepository.findAll();
            assertThat(records).isEmpty();  // Database is clean!
        }
    }


    @Nested
    @DisplayName("Performance Tests")
    class PerformanceTests {

        @Test
        @DisplayName("Should handle bulk insert and query efficiently")
        void testBulkOperations() throws Exception {
            // Given: Insert 50 records
            long insertStart = System.currentTimeMillis();
            for (int i = 0; i < 50; i++) {
                insertRecord(DATASET_NAME, Map.of(
                        "id", i,
                        "name", "User" + i,
                        "age", 20 + (i % 30),
                        "status", i % 3 == 0 ? "active" : (i % 3 == 1 ? "inactive" : "pending")
                ));
            }
            long insertEnd = System.currentTimeMillis();
            System.out.println("Insert 50 records took: " + (insertEnd - insertStart) + "ms");

            // When: Query with groupBy
            long queryStart = System.currentTimeMillis();
            mockMvc.perform(
                    get("/api/datasets/{datasetName}/query", DATASET_NAME)
                            .param("groupBy", "status")
            )
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.total_records").value(50));
            long queryEnd = System.currentTimeMillis();
            System.out.println("Group-by query on 50 records took: " + (queryEnd - queryStart) + "ms");

            // Performance assertions (reasonable thresholds)
            assertThat(insertEnd - insertStart).isLessThan(10000);  // < 10 seconds for 50 inserts
            assertThat(queryEnd - queryStart).isLessThan(2000);     // < 2 seconds for query
        }
    }

    

    @Test
    @DisplayName("Should return health status")
    void testHealthEndpoint() throws Exception {
        mockMvc.perform(get("/api/datasets/health"))
                .andDo(print())
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("UP"))
                .andExpect(jsonPath("$.message").value("Dataset API is running"));
    }
}
