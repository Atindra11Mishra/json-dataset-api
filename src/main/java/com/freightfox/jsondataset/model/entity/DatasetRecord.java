package com.freightfox.jsondataset.model.entity;

import io.hypersistence.utils.hibernate.type.json.JsonBinaryType;
import jakarta.persistence.*;
import lombok.*;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.Type;
import org.hibernate.annotations.UpdateTimestamp;

import java.time.OffsetDateTime;
import java.util.Map;
import java.util.UUID;

@Entity
@Table(
    name = "datasets",
    indexes = {
        @Index(name = "idx_datasets_name", columnList = "dataset_name"),
        @Index(name = "idx_datasets_name_active", columnList = "dataset_name, is_deleted")
    }
)
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
@ToString
public class DatasetRecord {

    
    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    @Column(name = "id", updatable = false, nullable = false, columnDefinition = "UUID")
    private UUID id;

    
    @Column(name = "dataset_name", nullable = false, length = 255)
    private String datasetName;

    
    @Type(JsonBinaryType.class)
    @Column(name = "data", nullable = false, columnDefinition = "jsonb")
    private Map<String, Object> data;

    
    @CreationTimestamp
    @Column(name = "created_at", nullable = false, updatable = false)
    private OffsetDateTime createdAt;

    
    @UpdateTimestamp
    @Column(name = "updated_at", nullable = false)
    private OffsetDateTime updatedAt;

    
    @Column(name = "is_deleted", nullable = false)
    @Builder.Default
    private Boolean isDeleted = false;

    
    @Version
    @Column(name = "version", nullable = false)
    @Builder.Default
    private Integer version = 1;

    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof DatasetRecord)) return false;
        DatasetRecord that = (DatasetRecord) o;
        return id != null && id.equals(that.id);
    }

    @Override
    public int hashCode() {
        return getClass().hashCode();
    }
}