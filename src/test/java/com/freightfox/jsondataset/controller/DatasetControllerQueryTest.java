package com.freightfox.jsondataset.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.freightfox.jsondataset.dto.request.InsertRecordRequest;
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
import org.springframework.transaction.annotation.Transactional;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultHandlers.print;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

/**
 * Integration tests for Query endpoint (group-by and sort-by).
 */
@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles("test")
@Transactional
@DisplayName("DatasetController Query Endpoint Tests")
class DatasetControllerQueryTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private DatasetRepository datasetRepository;

    @BeforeEach
    void setUp() throws Exception {
        // Clean database
        datasetRepository.deleteAll();

        // Insert test data: 6 users with different statuses and ages
        insertTestRecord("users", Map.of("name", "John", "age", 30, "status", "active"));
        insertTestRecord("users", Map.of("name", "Jane", "age", 25, "status", "active"));
        insertTestRecord("users", Map.of("name", "Bob", "age", 45, "status", "inactive"));
        insertTestRecord("users", Map.of("name", "Alice", "age", 35, "status", "pending"));
        insertTestRecord("users", Map.of("name", "Charlie", "age", 28, "status", "active"));
        insertTestRecord("users", Map.of("name", "Diana", "age", 40, "status", "inactive"));
    }

    private void insertTestRecord(String datasetName, Map<String, Object> data) throws Exception {
        InsertRecordRequest request = InsertRecordRequest.builder()
                .datasetName(datasetName)
                .data(data)
                .build();

        mockMvc.perform(
                post("/api/datasets/" + datasetName + "/record")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request))
        ).andExpect(status().isCreated());
    }

    // ==================== GROUP-BY TESTS ====================

    @Test
    @DisplayName("Should group records by status")
    void testQuery_GroupBy() throws Exception {
        // When: Query with groupBy=status
        mockMvc.perform(
                get("/api/datasets/users/query")
                        .param("groupBy", "status")
        )
        .andDo(print())
        .andExpect(status().isOk())  // 200
        .andExpect(jsonPath("$.success").value(true))
        .andExpect(jsonPath("$.operation").value("group_by"))
        .andExpect(jsonPath("$.field").value("status"))
        .andExpect(jsonPath("$.total_records").value(6))
        .andExpect(jsonPath("$.groups").exists())
        .andExpect(jsonPath("$.groups.active").isArray())
        .andExpect(jsonPath("$.groups.active.length()").value(3))  // John, Jane, Charlie
        .andExpect(jsonPath("$.groups.inactive.length()").value(2))  // Bob, Diana
        .andExpect(jsonPath("$.groups.pending.length()").value(1))  // Alice
        .andExpect(jsonPath("$.metadata.total_groups").value(3));
    }

    @Test
    @DisplayName("Should handle groupBy on non-existent field")
    void testQuery_GroupBy_MissingField() throws Exception {
        // When: Group by field that doesn't exist in all records
        mockMvc.perform(
                get("/api/datasets/users/query")
                        .param("groupBy", "department")  // Field doesn't exist
        )
        .andDo(print())
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.success").value(true))
        .andExpect(jsonPath("$.groups.__missing__").exists())
        .andExpect(jsonPath("$.groups.__missing__.length()").value(6))  // All records missing
        .andExpect(jsonPath("$.metadata.records_with_missing_field").value(6));
    }

    

    @Test
    @DisplayName("Should sort records by age ascending")
    void testQuery_SortBy_Ascending() throws Exception {
        // When: Query with sortBy=age&order=asc
        mockMvc.perform(
                get("/api/datasets/users/query")
                        .param("sortBy", "age")
                        .param("order", "asc")
        )
        .andDo(print())
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.success").value(true))
        .andExpect(jsonPath("$.operation").value("sort_by"))
        .andExpect(jsonPath("$.field").value("age"))
        .andExpect(jsonPath("$.total_records").value(6))
        .andExpect(jsonPath("$.results").isArray())
        .andExpect(jsonPath("$.results.length()").value(6))
        // Verify order: 25, 28, 30, 35, 40, 45
        .andExpect(jsonPath("$.results[0].age").value(25))  // Jane
        .andExpect(jsonPath("$.results[1].age").value(28))  // Charlie
        .andExpect(jsonPath("$.results[2].age").value(30))  // John
        .andExpect(jsonPath("$.results[3].age").value(35))  // Alice
        .andExpect(jsonPath("$.results[4].age").value(40))  // Diana
        .andExpect(jsonPath("$.results[5].age").value(45))  // Bob
        .andExpect(jsonPath("$.sort_metadata.sort_order").value("asc"))
        .andExpect(jsonPath("$.sort_metadata.field_type").value("numeric"));
    }

    @Test
    @DisplayName("Should sort records by age descending")
    void testQuery_SortBy_Descending() throws Exception {
        // When: Query with sortBy=age&order=desc
        mockMvc.perform(
                get("/api/datasets/users/query")
                        .param("sortBy", "age")
                        .param("order", "desc")
        )
        .andDo(print())
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.operation").value("sort_by"))
        .andExpect(jsonPath("$.results[0].age").value(45))  // Bob
        .andExpect(jsonPath("$.results[1].age").value(40))  // Diana
        .andExpect(jsonPath("$.results[5].age").value(25))  // Jane
        .andExpect(jsonPath("$.sort_metadata.sort_order").value("desc"));
    }

    @Test
    @DisplayName("Should sort by string field (name)")
    void testQuery_SortBy_String() throws Exception {
        // When: Sort by name (string field)
        mockMvc.perform(
                get("/api/datasets/users/query")
                        .param("sortBy", "name")
                        .param("order", "asc")
        )
        .andDo(print())
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.operation").value("sort_by"))
        .andExpect(jsonPath("$.results[0].name").value("Alice"))
        .andExpect(jsonPath("$.results[1].name").value("Bob"))
        .andExpect(jsonPath("$.results[2].name").value("Charlie"))
        .andExpect(jsonPath("$.sort_metadata.field_type").value("string"));
    }

    @Test
    @DisplayName("Should default to ascending order when order not specified")
    void testQuery_SortBy_DefaultOrder() throws Exception {
        // When: Sort without order parameter
        mockMvc.perform(
                get("/api/datasets/users/query")
                        .param("sortBy", "age")
                        // No order param - should default to asc
        )
        .andDo(print())
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.results[0].age").value(25))
        .andExpect(jsonPath("$.sort_metadata.sort_order").value("asc"));
    }

    // ==================== GROUP-BY-THEN-SORT TESTS ====================

    @Test
    @DisplayName("Should group by status, then sort by age within each group")
    void testQuery_GroupByThenSort() throws Exception {
        // When: Both groupBy and sortBy provided
        mockMvc.perform(
                get("/api/datasets/users/query")
                        .param("groupBy", "status")
                        .param("sortBy", "age")
                        .param("order", "asc")
        )
        .andDo(print())
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.success").value(true))
        .andExpect(jsonPath("$.operation").value("group_by_then_sort"))
        .andExpect(jsonPath("$.field").value("status"))
        .andExpect(jsonPath("$.groups").exists())
        // Verify active group sorted by age: 25, 28, 30
        .andExpect(jsonPath("$.groups.active[0].age").value(25))  // Jane
        .andExpect(jsonPath("$.groups.active[1].age").value(28))  // Charlie
        .andExpect(jsonPath("$.groups.active[2].age").value(30))  // John
        // Verify inactive group sorted by age: 40, 45
        .andExpect(jsonPath("$.groups.inactive[0].age").value(40))  // Diana
        .andExpect(jsonPath("$.groups.inactive[1].age").value(45))  // Bob
        // Verify metadata
        .andExpect(jsonPath("$.metadata.total_groups").value(3))
        .andExpect(jsonPath("$.sort_metadata.sort_order").value("asc"));
    }

    @Test
    @DisplayName("Should handle group-by-then-sort with descending order")
    void testQuery_GroupByThenSort_Descending() throws Exception {
        // When: Group by status, sort by age descending
        mockMvc.perform(
                get("/api/datasets/users/query")
                        .param("groupBy", "status")
                        .param("sortBy", "age")
                        .param("order", "desc")
        )
        .andDo(print())
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.operation").value("group_by_then_sort"))
        // Verify active group sorted descending: 30, 28, 25
        .andExpect(jsonPath("$.groups.active[0].age").value(30))  // John
        .andExpect(jsonPath("$.groups.active[1].age").value(28))  // Charlie
        .andExpect(jsonPath("$.groups.active[2].age").value(25))  // Jane
        .andExpect(jsonPath("$.sort_metadata.sort_order").value("desc"));
    }

    // ==================== VALIDATION ERROR TESTS ====================

    @Test
    @DisplayName("Should return 400 when neither groupBy nor sortBy provided")
    void testQuery_NoParameters() throws Exception {
        // When: No query parameters
        mockMvc.perform(
                get("/api/datasets/users/query")
                // No params
        )
        .andDo(print())
        .andExpect(status().isBadRequest())  // 400
        .andExpect(jsonPath("$.status").value(400))
        .andExpect(jsonPath("$.code").value("INVALID_JSON"))
        .andExpect(jsonPath("$.message").value("At least one query parameter (groupBy or sortBy) must be provided"));
    }

    @Test
    @DisplayName("Should return 400 for invalid sort order")
    void testQuery_InvalidSortOrder() throws Exception {
        // When: Invalid sort order
        mockMvc.perform(
                get("/api/datasets/users/query")
                        .param("sortBy", "age")
                        .param("order", "invalid")  // Not asc or desc
        )
        .andDo(print())
        .andExpect(status().isBadRequest())
        .andExpect(jsonPath("$.status").value(400))
        .andExpect(jsonPath("$.code").value("INVALID_SORT_ORDER"));
    }

    @Test
    @DisplayName("Should return empty result for non-existent dataset")
    void testQuery_EmptyDataset() throws Exception {
        // When: Query non-existent dataset
        mockMvc.perform(
                get("/api/datasets/nonexistent/query")
                        .param("groupBy", "status")
        )
        .andDo(print())
        .andExpect(status().isOk())  // 200 with empty result
        .andExpect(jsonPath("$.success").value(true))
        .andExpect(jsonPath("$.total_records").value(0))
        .andExpect(jsonPath("$.groups").isEmpty());
    }

    // ==================== EDGE CASES ====================

    @Test
    @DisplayName("Should handle case-insensitive order parameter")
    void testQuery_CaseInsensitiveOrder() throws Exception {
        // When: Order with different cases
        mockMvc.perform(
                get("/api/datasets/users/query")
                        .param("sortBy", "age")
                        .param("order", "DESC")  // Uppercase
        )
        .andDo(print())
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.sort_metadata.sort_order").value("desc"));

        mockMvc.perform(
                get("/api/datasets/users/query")
                        .param("sortBy", "age")
                        .param("order", "AsC")  // Mixed case
        )
        .andDo(print())
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.sort_metadata.sort_order").value("asc"));
    }

    @Test
    @DisplayName("Should handle nested field in groupBy")
    void testQuery_NestedFieldGroupBy() throws Exception {
        // Setup: Add records with nested address field
        datasetRepository.deleteAll();
        insertTestRecord("users", Map.of("name", "User1", 
                "address", Map.of("city", "New York")));
        insertTestRecord("users", Map.of("name", "User2", 
                "address", Map.of("city", "New York")));
        insertTestRecord("users", Map.of("name", "User3", 
                "address", Map.of("city", "Los Angeles")));

        // When: Group by nested field
        mockMvc.perform(
                get("/api/datasets/users/query")
                        .param("groupBy", "address.city")
        )
        .andDo(print())
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.groups['New York'].length()").value(2))
        .andExpect(jsonPath("$.groups['Los Angeles'].length()").value(1));
    }

    @Test
    @DisplayName("Should handle URL encoding in field names")
    void testQuery_URLEncodedFieldNames() throws Exception {
        // Setup: Add record with space in field name (unusual but valid)
        datasetRepository.deleteAll();
        insertTestRecord("users", Map.of("user name", "John", "age", 30));

        // When: Query with URL-encoded field name
        mockMvc.perform(
                get("/api/datasets/users/query")
                        .param("sortBy", "user name")  // Spring auto-decodes
        )
        .andDo(print())
        .andExpect(status().isOk());
    }

    // ==================== PERFORMANCE TEST ====================

    @Test
    @DisplayName("Should handle query on large dataset efficiently")
    void testQuery_LargeDataset() throws Exception {
        // Setup: Insert 100 records
        datasetRepository.deleteAll();
        for (int i = 0; i < 100; i++) {
            insertTestRecord("users", Map.of(
                    "id", i,
                    "status", i % 3 == 0 ? "active" : (i % 3 == 1 ? "inactive" : "pending"),
                    "score", i * 10
            ));
        }

        // When: Group by status (should have 3 groups)
        long startTime = System.currentTimeMillis();
        mockMvc.perform(
                get("/api/datasets/users/query")
                        .param("groupBy", "status")
        )
        .andDo(print())
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.metadata.total_groups").value(3))
        .andExpect(jsonPath("$.total_records").value(100));
        long endTime = System.currentTimeMillis();

        System.out.println("Query on 100 records took: " + (endTime - startTime) + "ms");
        
        // Performance check: should complete in < 2 seconds
        assertThat(endTime - startTime).isLessThan(2000);
    }
}
