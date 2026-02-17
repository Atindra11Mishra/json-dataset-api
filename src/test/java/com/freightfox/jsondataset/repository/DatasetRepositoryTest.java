package com.freightfox.jsondataset.repository;

import com.freightfox.jsondataset.model.entity.DatasetRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.test.context.ActiveProfiles;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.*;

@DataJpaTest
@ActiveProfiles("test")
class DatasetRepositoryTest {

    @Autowired
    private DatasetRepository datasetRepository;

    @BeforeEach
    void setUp() {
        datasetRepository.deleteAll();
    }

    private DatasetRecord createRecord(String datasetName, Map<String, Object> data) {
        return DatasetRecord.builder()
                .datasetName(datasetName)
                .data(data)
                .isDeleted(false)
                .build();
    }

    @Test
    @DisplayName("Should find records by dataset name excluding deleted")
    void findByDatasetNameAndIsDeleted_returnsActiveRecords() {
        datasetRepository.save(createRecord("sales", Map.of("amount", 100)));
        datasetRepository.save(createRecord("sales", Map.of("amount", 200)));
        DatasetRecord deleted = createRecord("sales", Map.of("amount", 300));
        deleted.setIsDeleted(true);
        datasetRepository.save(deleted);

        List<DatasetRecord> results = datasetRepository.findByDatasetNameAndIsDeleted("sales", false);

        assertThat(results).hasSize(2);
    }

    @Test
    @DisplayName("Should count active records in a dataset")
    void countByDatasetNameAndIsDeleted_countsActiveOnly() {
        datasetRepository.save(createRecord("orders", Map.of("id", 1)));
        datasetRepository.save(createRecord("orders", Map.of("id", 2)));
        DatasetRecord deleted = createRecord("orders", Map.of("id", 3));
        deleted.setIsDeleted(true);
        datasetRepository.save(deleted);

        long count = datasetRepository.countByDatasetNameAndIsDeleted("orders", false);

        assertThat(count).isEqualTo(2);
    }

    @Test
    @DisplayName("Should check if dataset exists")
    void existsByDatasetNameAndIsDeleted_returnsTrueWhenExists() {
        datasetRepository.save(createRecord("inventory", Map.of("item", "widget")));

        assertThat(datasetRepository.existsByDatasetNameAndIsDeleted("inventory", false)).isTrue();
        assertThat(datasetRepository.existsByDatasetNameAndIsDeleted("nonexistent", false)).isFalse();
    }

    @Test
    @DisplayName("Should find latest record by dataset name")
    void findFirstByDatasetNameOrderByCreatedAtDesc_returnsLatest() throws InterruptedException {
        datasetRepository.save(createRecord("logs", Map.of("msg", "first")));
        datasetRepository.flush();
        Thread.sleep(10);
        DatasetRecord latest = datasetRepository.save(createRecord("logs", Map.of("msg", "second")));
        datasetRepository.flush();

        Optional<DatasetRecord> result = datasetRepository
                .findFirstByDatasetNameAndIsDeletedOrderByCreatedAtDesc("logs", false);

        assertThat(result).isPresent();
        assertThat(result.get().getId()).isEqualTo(latest.getId());
    }

    @Test
    @DisplayName("Should return empty when no records exist")
    void findFirstByDatasetName_returnsEmptyWhenNoRecords() {
        Optional<DatasetRecord> result = datasetRepository
                .findFirstByDatasetNameAndIsDeletedOrderByCreatedAtDesc("empty", false);

        assertThat(result).isEmpty();
    }
}
