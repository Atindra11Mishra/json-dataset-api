package com.freightfox.jsondataset.repository;

import com.freightfox.jsondataset.model.entity.DatasetRecord;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Repository
public interface DatasetRepository extends JpaRepository<DatasetRecord, UUID> {

    
    List<DatasetRecord> findByDatasetNameAndIsDeleted(String datasetName, Boolean isDeleted);

    
    long countByDatasetNameAndIsDeleted(String datasetName, Boolean isDeleted);

    
    boolean existsByDatasetNameAndIsDeleted(String datasetName, Boolean isDeleted);

    
    @Query(value = """
        SELECT * FROM datasets d
        WHERE d.dataset_name = :datasetName
          AND d.is_deleted = false
        ORDER BY d.data ->> :orderField ASC
        """, nativeQuery = true)
    List<DatasetRecord> findByDatasetNameOrderByJsonField(
        @Param("datasetName") String datasetName,
        @Param("orderField") String orderField
    );

    
    List<DatasetRecord> findByDatasetNameAndIsDeletedAndCreatedAtBetween(
        String datasetName,
        Boolean isDeleted,
        OffsetDateTime startDate,
        OffsetDateTime endDate
    );

    
    Optional<DatasetRecord> findFirstByDatasetNameAndIsDeletedOrderByCreatedAtDesc(
        String datasetName,
        Boolean isDeleted
    );

    
    @Query(value = """
        SELECT * FROM datasets 
        WHERE dataset_name = :datasetName 
        AND is_deleted = false 
        AND CAST(data AS jsonb) @> CAST(:jsonFilter AS jsonb)
        """, 
        nativeQuery = true)
    List<DatasetRecord> findByDatasetNameAndJsonContains(
        @Param("datasetName") String datasetName,
        @Param("jsonFilter") String jsonFilter
    );

    
    @Query("UPDATE DatasetRecord d SET d.isDeleted = true WHERE d.datasetName = :datasetName")
    @org.springframework.data.jpa.repository.Modifying(clearAutomatically = true)
    int softDeleteByDatasetName(@Param("datasetName") String datasetName);
}
