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

import java.util.HashMap;
import java.util.Map;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultHandlers.print;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles("test")
@Transactional
@DisplayName("DatasetController Query Endpoint Integration Tests")
class DatasetControllerIntegrationTest_Query {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private DatasetRepository datasetRepository;

    @BeforeEach
    void setUp() throws Exception {
        datasetRepository.deleteAll();

        // Insert sample data
        insertRecord("users", "John", 30, "active");
        insertRecord("users", "Jane", 25, "active");
        insertRecord("users", "Bob", 40, "inactive");
    }

    private void insertRecord(String datasetName, String name, int age, String status) throws Exception {
        Map<String, Object> data = new HashMap<>();
        data.put("name", name);
        data.put("age", age);
        data.put("status", status);

        InsertRecordRequest request = InsertRecordRequest.builder()
                .datasetName(datasetName)
                .data(data)
                .build();

        mockMvc.perform(post("/api/datasets/" + datasetName + "/record")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isCreated());
    }

    @Test
    @DisplayName("Should execute group-by-then-sort query")
    void testQuery_GroupByThenSort() throws Exception {
        mockMvc.perform(get("/api/datasets/users/query")
                .param("groupBy", "status")
                .param("sortBy", "age")
                .param("order", "asc"))
                .andDo(print())
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.success").value(true))
                .andExpect(jsonPath("$.operation").value("group_by"))
                .andExpect(jsonPath("$.groups.active").isArray())
                .andExpect(jsonPath("$.groups.active[0].name").value("Jane")) // 25 < 30
                .andExpect(jsonPath("$.groups.active[1].name").value("John"));
    }

    @Test
    @DisplayName("Should execute group-by only query")
    void testQuery_GroupByOnly() throws Exception {
        mockMvc.perform(get("/api/datasets/users/query")
                .param("groupBy", "status"))
                .andDo(print())
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.success").value(true))
                .andExpect(jsonPath("$.operation").value("group_by"))
                // Sorting within group is not guaranteed if not requested, but grouping must
                // exist
                .andExpect(jsonPath("$.groups.active").exists())
                .andExpect(jsonPath("$.groups.inactive").exists());
    }

    @Test
    @DisplayName("Should execute sort-by only query")
    void testQuery_SortByOnly() throws Exception {
        mockMvc.perform(get("/api/datasets/users/query")
                .param("sortBy", "age")
                .param("order", "desc"))
                .andDo(print())
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.success").value(true))
                .andExpect(jsonPath("$.operation").value("sort_by"))
                .andExpect(jsonPath("$.results[0].name").value("Bob")) // 40
                .andExpect(jsonPath("$.results[1].name").value("John")); // 30
    }

    @Test
    @DisplayName("Should return 400 if neither parameter provided")
    void testQuery_MissingParams() throws Exception {
        mockMvc.perform(get("/api/datasets/users/query"))
                .andDo(print())
                .andExpect(status().isBadRequest());
    }
}
