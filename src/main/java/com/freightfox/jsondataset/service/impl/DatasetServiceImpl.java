package com.freightfox.jsondataset.service.impl;

import com.freightfox.jsondataset.exception.InvalidSortOrderException;
import com.freightfox.jsondataset.model.enums.SortOrder;
import java.math.BigDecimal;
import com.freightfox.jsondataset.dto.request.InsertRecordRequest;
import com.freightfox.jsondataset.dto.request.QueryRequest;
import com.freightfox.jsondataset.dto.response.InsertRecordResponse;
import com.freightfox.jsondataset.dto.response.QueryResponse;
import com.freightfox.jsondataset.exception.DatasetNotFoundException;
import com.freightfox.jsondataset.exception.DatasetValidationException;
import com.freightfox.jsondataset.exception.InvalidFieldException;
import com.freightfox.jsondataset.exception.InvalidJsonException;
import com.freightfox.jsondataset.model.entity.DatasetRecord;
import com.freightfox.jsondataset.repository.DatasetRepository;
import com.freightfox.jsondataset.service.DatasetService;
import com.freightfox.jsondataset.util.JsonValidator;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.*;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
@Slf4j
@Transactional
public class DatasetServiceImpl implements DatasetService {

    private final DatasetRepository datasetRepository;
    private final JsonValidator jsonValidator;

    private static final String MISSING_FIELD_KEY = "__missing__";
    private static final String NULL_FIELD_KEY = "__null__";

    @Override
    @Transactional
    public InsertRecordResponse insertRecord(InsertRecordRequest request) {
        String datasetNameForLog = request != null ? request.getDatasetName() : null;
        log.info("Inserting record into dataset: {}", datasetNameForLog);

        try {
            validateRequest(request);
            validateDatasetName(request.getDatasetName());
            validateJsonData(request.getData());

            DatasetRecord record = buildEntityFromRequest(request);
            DatasetRecord savedRecord = datasetRepository.save(record);

            log.info("Record inserted successfully. ID: {}, Dataset: {}",
                    savedRecord.getId(), savedRecord.getDatasetName());

            return buildSuccessResponse(savedRecord);

        } catch (InvalidJsonException | DatasetValidationException e) {
            log.warn("Validation failed for dataset {}: {}",
                    datasetNameForLog, e.getMessage());
            throw e;

        } catch (Exception e) {
            log.error("Unexpected error inserting record into dataset {}",
                    datasetNameForLog, e);
            throw new RuntimeException("Failed to insert record: " + e.getMessage(), e);
        }
    }

    
    @Override
    @Transactional(readOnly = true)
    public QueryResponse groupBy(QueryRequest request) {
        String datasetNameForLog = request != null ? request.getDatasetName() : null;
        String groupByForLog = request != null ? request.getGroupBy() : null;
        log.info("Executing group-by query on dataset: {}, field: {}",
                datasetNameForLog, groupByForLog);

        try {
            validateQueryRequest(request);
            String datasetName = request.getDatasetName();
            String groupByField = request.getGroupBy();

            List<DatasetRecord> records = datasetRepository
                    .findByDatasetNameAndIsDeleted(datasetName, false);

            if (records.isEmpty()) {
                log.warn("Dataset '{}' has no records or doesn't exist", datasetName);
                return buildEmptyQueryResponse(datasetName, groupByField);
            }

            log.debug("Found {} records in dataset '{}'", records.size(), datasetName);

            GroupingResult groupingResult = groupRecordsByField(records, groupByField);

            QueryResponse.QueryMetadata metadata = buildGroupingMetadata(groupingResult);

            QueryResponse response = QueryResponse.groupBySuccess(
                    datasetName,
                    groupByField,
                    null,
                    groupingResult.getGroups(),
                    metadata);

            log.info("Group-by completed. Dataset: {}, Groups: {}, Total records: {}",
                    datasetName, metadata.getTotalGroups(), response.getTotalRecords());

            return response;

        } catch (DatasetNotFoundException | InvalidFieldException | InvalidJsonException e) {
            log.warn("Query validation failed: {}", e.getMessage());
            throw e;

        } catch (Exception e) {
            log.error("Unexpected error during group-by query on dataset {}",
                    datasetNameForLog, e);
            throw new RuntimeException("Failed to execute group-by query: " + e.getMessage(), e);
        }
    }

    
    @Override
    @Transactional(readOnly = true)
    public QueryResponse groupByThenSort(QueryRequest request) {
        String datasetNameForLog = request != null ? request.getDatasetName() : null;
        String groupByForLog = request != null ? request.getGroupBy() : null;
        String sortByForLog = request != null ? request.getSortBy() : null;
        String sortOrderForLog = request != null ? request.getSortOrder() : null;
        log.info("Executing group-by-then-sort query on dataset: {}, groupBy: {}, sortBy: {}, order: {}",
                datasetNameForLog, groupByForLog, sortByForLog, sortOrderForLog);

        try {
            validateGroupByThenSortRequest(request);
            String datasetName = request.getDatasetName();
            String groupByField = request.getGroupBy();
            String sortByField = request.getSortBy();
            SortOrder sortOrder = SortOrder.fromString(request.getSortOrder());

            List<DatasetRecord> records = datasetRepository
                    .findByDatasetNameAndIsDeleted(datasetName, false);

            if (records.isEmpty()) {
                log.warn("Dataset '{}' has no records or doesn't exist", datasetName);
                return buildEmptyGroupByThenSortResponse(datasetName, groupByField, sortByField, sortOrder);
            }

            log.debug("Found {} records in dataset '{}'", records.size(), datasetName);

            GroupingResult groupingResult = groupRecordsByField(records, groupByField);
            Map<String, List<Map<String, Object>>> groups = groupingResult.getGroups();

            groups.forEach((groupKey, groupRecords) -> {
                List<SortableValue> sortableValues = groupRecords.stream()
                        .map(jsonData -> {
                            FieldExtractionResult extraction = extractFieldValue(jsonData, sortByField);
                            return SortableValue.builder()
                                    .jsonData(jsonData)
                                    .fieldValue(extraction.getFieldValue())
                                    .fieldExists(extraction.isFieldExists())
                                    .valueType(determineValueType(extraction.getFieldValue()))
                                    .build();
                        })
                        .collect(Collectors.toList());

                FieldTypeInfo fieldTypeInfo = detectFieldType(sortableValues);

                sortValues(sortableValues, sortOrder, fieldTypeInfo);

                List<Map<String, Object>> sortedRecords = sortableValues.stream()
                        .map(SortableValue::getJsonData)
                        .collect(Collectors.toList());

                groups.put(groupKey, sortedRecords);
            });

            QueryResponse.QueryMetadata groupMetadata = buildGroupingMetadata(groupingResult);

            int totalMissingSortField = 0;
            int totalNullSortField = 0;

            for (List<Map<String, Object>> groupRecords : groups.values()) {
                for (Map<String, Object> record : groupRecords) {
                    FieldExtractionResult extraction = extractFieldValue(record, sortByField);
                    if (!extraction.isFieldExists()) {
                        totalMissingSortField++;
                    } else if (extraction.getFieldValue() == null) {
                        totalNullSortField++;
                    }
                }
            }

            QueryResponse response = QueryResponse.builder()
                    .success(true)
                    .datasetName(datasetName)
                    .operation("group_by_then_sort")
                    .field(groupByField)
                    .sortField(sortByField)
                    .totalRecords(records.size())
                    .groups(groups)
                    .metadata(QueryResponse.QueryMetadata.builder()
                            .totalGroups(groupMetadata.getTotalGroups())
                            .recordsWithMissingField(groupMetadata.getRecordsWithMissingField())
                            .recordsWithNullField(groupMetadata.getRecordsWithNullField())
                            .groupSizes(groupMetadata.getGroupSizes())
                            .build())
                    .sortMetadata(QueryResponse.SortMetadata.builder()
                            .sortOrder(sortOrder.getValue())
                            .fieldType("mixed")
                            .recordsWithMissingField(totalMissingSortField)
                            .recordsWithNullField(totalNullSortField)
                            .recordsWithTypeMismatch(0)
                            .warnings(List.of(String.format(
                                    "Records grouped by '%s', then sorted by '%s' within each group",
                                    groupByField, sortByField)))
                            .build())
                    .build();

            log.info("Group-by-then-sort completed. Dataset: {}, Groups: {}, Total records: {}",
                    datasetName, groupMetadata.getTotalGroups(), records.size());

            return response;

        } catch (DatasetNotFoundException | InvalidFieldException | InvalidSortOrderException | InvalidJsonException e) {
            log.warn("Query validation failed: {}", e.getMessage());
            throw e;

        } catch (Exception e) {
            log.error("Unexpected error during group-by-then-sort query on dataset {}",
                    datasetNameForLog, e);
            throw new RuntimeException("Failed to execute group-by-then-sort query: " + e.getMessage(), e);
        }
    }

    
    private void validateGroupByThenSortRequest(QueryRequest request) {
        if (request == null) {
            throw new InvalidJsonException("Query request cannot be null");
        }

        if (!request.isValid()) {
            throw new InvalidJsonException("Query request is invalid");
        }

        if (!request.isGroupByQuery()) {
            throw new InvalidFieldException(
                    request.getGroupBy(),
                    request.getDatasetName(),
                    "Group-by field is required for group-by-then-sort operation");
        }

        if (!request.isSortByQuery()) {
            throw new InvalidFieldException(
                    request.getSortBy(),
                    request.getDatasetName(),
                    "Sort-by field is required for group-by-then-sort operation");
        }

        jsonValidator.validateDatasetName(request.getDatasetName());

        try {
            SortOrder.fromString(request.getSortOrder());
        } catch (IllegalArgumentException e) {
            throw new InvalidSortOrderException(
                    request.getSortOrder(),
                    request.getDatasetName(),
                    e.getMessage());
        }
    }

    
    private QueryResponse buildEmptyGroupByThenSortResponse(
            String datasetName,
            String groupByField,
            String sortByField,
            SortOrder sortOrder) {
        return QueryResponse.builder()
                .success(true)
                .datasetName(datasetName)
                .operation("group_by_then_sort")
                .field(groupByField)
                .totalRecords(0)
                .groups(Collections.emptyMap())
                .metadata(QueryResponse.QueryMetadata.builder()
                        .totalGroups(0)
                        .recordsWithMissingField(0)
                        .recordsWithNullField(0)
                        .groupSizes(Collections.emptyMap())
                        .build())
                .sortMetadata(QueryResponse.SortMetadata.builder()
                        .sortOrder(sortOrder.getValue())
                        .fieldType("unknown")
                        .recordsWithMissingField(0)
                        .recordsWithNullField(0)
                        .recordsWithTypeMismatch(0)
                        .warnings(List.of("Dataset is empty"))
                        .build())
                .build();
    }

    
    @Override
    @Transactional(readOnly = true)
    public QueryResponse sortBy(QueryRequest request) {
        String datasetNameForLog = request != null ? request.getDatasetName() : null;
        String sortByForLog = request != null ? request.getSortBy() : null;
        String sortOrderForLog = request != null ? request.getSortOrder() : null;
        log.info("Executing sort-by query on dataset: {}, field: {}, order: {}",
                datasetNameForLog, sortByForLog, sortOrderForLog);

        try {
            validateSortByRequest(request);
            String datasetName = request.getDatasetName();
            String sortByField = request.getSortBy();
            SortOrder sortOrder = SortOrder.fromString(request.getSortOrder());

            List<DatasetRecord> records = datasetRepository
                    .findByDatasetNameAndIsDeleted(datasetName, false);

            if (records.isEmpty()) {
                log.warn("Dataset '{}' has no records or doesn't exist", datasetName);
                return buildEmptySortResponse(datasetName, sortByField, sortOrder);
            }

            log.debug("Found {} records in dataset '{}'", records.size(), datasetName);

            List<SortableValue> sortableValues = extractSortableValues(records, sortByField);

            FieldTypeInfo fieldTypeInfo = detectFieldType(sortableValues);
            log.debug("Detected field type: {}", fieldTypeInfo.getFieldType());

            sortValues(sortableValues, sortOrder, fieldTypeInfo);

            List<Map<String, Object>> sortedResults = sortableValues.stream()
                    .map(SortableValue::getJsonData)
                    .collect(Collectors.toList());

            QueryResponse.SortMetadata metadata = buildSortMetadata(
                    sortableValues, fieldTypeInfo, sortOrder);

            QueryResponse response = QueryResponse.sortBySuccess(
                    datasetName,
                    sortByField,
                    sortOrder.getValue(),
                    sortedResults,
                    metadata);

            log.info("Sort-by completed. Dataset: {}, Records: {}, Type: {}",
                    datasetName, sortedResults.size(), fieldTypeInfo.getFieldType());

            return response;

        } catch (DatasetNotFoundException | InvalidFieldException | InvalidSortOrderException | InvalidJsonException e) {
            log.warn("Sort-by validation failed: {}", e.getMessage());
            throw e;

        } catch (Exception e) {
            log.error("Unexpected error during sort-by query on dataset {}",
                    datasetNameForLog, e);
            throw new RuntimeException("Failed to execute sort-by query: " + e.getMessage(), e);
        }
    }

    
    private GroupingResult groupRecordsByField(List<DatasetRecord> records, String groupByField) {
        log.debug("Grouping {} records by field '{}'", records.size(), groupByField);

        int missingFieldCount = 0;
        int nullFieldCount = 0;

        Map<String, List<Map<String, Object>>> groups = new LinkedHashMap<>();

        for (DatasetRecord record : records) {
            Map<String, Object> jsonData = record.getData();

            FieldExtractionResult extractionResult = extractFieldValue(jsonData, groupByField);

            String groupKey;
            if (!extractionResult.isFieldExists()) {
                groupKey = MISSING_FIELD_KEY;
                missingFieldCount++;
                log.trace("Record {} missing field '{}'", record.getId(), groupByField);
            } else if (extractionResult.getFieldValue() == null) {
                groupKey = NULL_FIELD_KEY;
                nullFieldCount++;
                log.trace("Record {} has null value for field '{}'", record.getId(), groupByField);
            } else {
                groupKey = convertValueToGroupKey(extractionResult.getFieldValue());
                log.trace("Record {} grouped under key '{}'", record.getId(), groupKey);
            }

            groups.computeIfAbsent(groupKey, k -> new ArrayList<>()).add(jsonData);
        }

        log.debug("Grouping complete. Total groups: {}, Missing field: {}, Null field: {}",
                groups.size(), missingFieldCount, nullFieldCount);

        return GroupingResult.builder()
                .groups(groups)
                .missingFieldCount(missingFieldCount)
                .nullFieldCount(nullFieldCount)
                .build();
    }

    
    private FieldExtractionResult extractFieldValue(Map<String, Object> data, String field) {
        if (field == null || field.trim().isEmpty()) {
            throw new InvalidFieldException(field, "", "Field name cannot be null or empty");
        }

        String[] fieldParts = field.split("\\.");
        Object currentValue = data;

        for (String part : fieldParts) {
            if (currentValue == null) {
                return FieldExtractionResult.notExists();
            }

            if (!(currentValue instanceof Map)) {
                return FieldExtractionResult.notExists();
            }

            @SuppressWarnings("unchecked")
            Map<String, Object> currentMap = (Map<String, Object>) currentValue;

            if (!currentMap.containsKey(part)) {
                return FieldExtractionResult.notExists();
            }

            currentValue = currentMap.get(part);
        }

        return FieldExtractionResult.exists(currentValue);
    }

    
    private String convertValueToGroupKey(Object value) {
        if (value == null) {
            return NULL_FIELD_KEY;
        }

        if (value instanceof String) {
            return (String) value;
        }

        if (value instanceof Number || value instanceof Boolean) {
            return value.toString();
        }

        if (value instanceof List) {
            List<?> list = (List<?>) value;
            return list.stream()
                    .map(Object::toString)
                    .collect(Collectors.joining(", "));
        }

        if (value instanceof Map) {
            return value.toString();
        }

        return value.toString();
    }

    
    private QueryResponse.QueryMetadata buildGroupingMetadata(GroupingResult result) {
        Map<String, List<Map<String, Object>>> groups = result.getGroups();

        Map<String, Integer> groupSizes = groups.entrySet().stream()
                .filter(entry -> !entry.getKey().equals(MISSING_FIELD_KEY)
                        && !entry.getKey().equals(NULL_FIELD_KEY))
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        entry -> entry.getValue().size()));

        int totalGroups = (int) groups.keySet().stream()
                .filter(key -> !key.equals(MISSING_FIELD_KEY) && !key.equals(NULL_FIELD_KEY))
                .count();

        return QueryResponse.QueryMetadata.builder()
                .totalGroups(totalGroups)
                .recordsWithMissingField(result.getMissingFieldCount())
                .recordsWithNullField(result.getNullFieldCount())
                .groupSizes(groupSizes)
                .build();
    }

    
    private QueryResponse buildEmptyQueryResponse(String datasetName, String field) {
        return QueryResponse.builder()
                .success(true)
                .datasetName(datasetName)
                .operation("group_by")
                .field(field)
                .totalRecords(0)
                .groups(Collections.emptyMap())
                .metadata(QueryResponse.QueryMetadata.builder()
                        .totalGroups(0)
                        .recordsWithMissingField(0)
                        .recordsWithNullField(0)
                        .groupSizes(Collections.emptyMap())
                        .build())
                .build();
    }

    
    private List<SortableValue> extractSortableValues(List<DatasetRecord> records, String field) {
        List<SortableValue> sortableValues = new ArrayList<>();

        for (DatasetRecord record : records) {
            FieldExtractionResult extraction = extractFieldValue(record.getData(), field);

            SortableValue sortableValue = SortableValue.builder()
                    .jsonData(record.getData())
                    .fieldValue(extraction.getFieldValue())
                    .fieldExists(extraction.isFieldExists())
                    .valueType(determineValueType(extraction.getFieldValue()))
                    .build();

            sortableValues.add(sortableValue);
        }

        return sortableValues;
    }

    
    private List<SortableValue> extractSortableValuesFromMaps(List<Map<String, Object>> dataList, String field) {
        List<SortableValue> sortableValues = new ArrayList<>();

        for (Map<String, Object> data : dataList) {
            FieldExtractionResult extraction = extractFieldValue(data, field);

            SortableValue sortableValue = SortableValue.builder()
                    .jsonData(data)
                    .fieldValue(extraction.getFieldValue())
                    .fieldExists(extraction.isFieldExists())
                    .valueType(determineValueType(extraction.getFieldValue()))
                    .build();

            sortableValues.add(sortableValue);
        }

        return sortableValues;
    }

    
    private ValueType determineValueType(Object value) {
        if (value == null) {
            return ValueType.NULL;
        }

        if (value instanceof Number) {
            return ValueType.NUMBER;
        }

        if (value instanceof Boolean) {
            return ValueType.BOOLEAN;
        }

        if (value instanceof String) {
            return ValueType.STRING;
        }

        if (value instanceof List) {
            return ValueType.ARRAY;
        }

        if (value instanceof Map) {
            return ValueType.OBJECT;
        }

        return ValueType.UNKNOWN;
    }

    
    private FieldTypeInfo detectFieldType(List<SortableValue> values) {
        Map<ValueType, Integer> typeCounts = new HashMap<>();
        int nullCount = 0;
        int missingCount = 0;
        int sampledCount = 0;
        final int SAMPLE_SIZE = 10;

        for (SortableValue value : values) {
            if (!value.isFieldExists()) {
                missingCount++;
                continue;
            }

            ValueType type = value.getValueType();

            if (type == ValueType.NULL) {
                nullCount++;
                continue;
            }

            typeCounts.put(type, typeCounts.getOrDefault(type, 0) + 1);
            sampledCount++;

            if (sampledCount >= SAMPLE_SIZE) {
                break;
            }
        }

        if (typeCounts.isEmpty()) {
            return FieldTypeInfo.builder()
                    .fieldType(FieldType.MIXED)
                    .isMixed(true)
                    .missingCount(missingCount)
                    .nullCount(nullCount)
                    .typeMismatchCount(0)
                    .warnings(List.of("All values are null or missing"))
                    .build();
        }

        ValueType predominantType = typeCounts.entrySet().stream()
                .max(Map.Entry.comparingByValue())
                .map(Map.Entry::getKey)
                .orElse(ValueType.UNKNOWN);

        int predominantCount = typeCounts.get(predominantType);
        boolean isMixed = typeCounts.size() > 1;

        int typeMismatchCount = sampledCount - predominantCount;

        FieldType fieldType;
        List<String> warnings = new ArrayList<>();

        if (!isMixed || (predominantCount >= sampledCount * 0.8)) {
            fieldType = mapValueTypeToFieldType(predominantType);
        } else {
            fieldType = FieldType.MIXED;
            warnings.add(String.format(
                    "Field contains mixed types (%s). Sorting as strings.",
                    formatTypeCounts(typeCounts)));
        }

        if (nullCount > 0) {
            warnings.add(String.format("%d records have null values (sorted to end)", nullCount));
        }

        if (missingCount > 0) {
            warnings.add(String.format("%d records missing the field (sorted to end)", missingCount));
        }

        return FieldTypeInfo.builder()
                .fieldType(fieldType)
                .isMixed(isMixed)
                .missingCount(missingCount)
                .nullCount(nullCount)
                .typeMismatchCount(typeMismatchCount)
                .warnings(warnings)
                .build();
    }

    
    private FieldType mapValueTypeToFieldType(ValueType valueType) {
        return switch (valueType) {
            case NUMBER -> FieldType.NUMERIC;
            case BOOLEAN -> FieldType.BOOLEAN;
            case STRING -> FieldType.STRING;
            default -> FieldType.MIXED;
        };
    }

    
    private String formatTypeCounts(Map<ValueType, Integer> typeCounts) {
        return typeCounts.entrySet().stream()
                .map(entry -> entry.getKey() + ":" + entry.getValue())
                .collect(Collectors.joining(", "));
    }

    
    private void sortValues(
            List<SortableValue> values,
            SortOrder sortOrder,
            FieldTypeInfo fieldTypeInfo) {
        Comparator<SortableValue> valueComparator = createComparator(fieldTypeInfo.getFieldType());

        values.sort((a, b) -> {
            int nullComparison = compareNullOrMissingLast(a, b);
            if (nullComparison != 0) {
                return nullComparison;
            }

            int comparison = valueComparator.compare(a, b);
            return sortOrder.isDescending() ? -comparison : comparison;
        });
    }

    private int compareNullOrMissingLast(SortableValue a, SortableValue b) {
        boolean aNullOrMissing = !a.isFieldExists() || a.getFieldValue() == null;
        boolean bNullOrMissing = !b.isFieldExists() || b.getFieldValue() == null;

        if (aNullOrMissing && bNullOrMissing) {
            return 0;
        }
        if (aNullOrMissing) {
            return 1;
        }
        if (bNullOrMissing) {
            return -1;
        }
        return 0;
    }

    
    private Comparator<SortableValue> createComparator(FieldType fieldType) {
        return switch (fieldType) {
            case NUMERIC -> createNumericComparator();
            case STRING -> createStringComparator();
            case BOOLEAN -> createBooleanComparator();
            case MIXED -> createMixedComparator();
        };
    }

    
    private Comparator<SortableValue> createNumericComparator() {
        return (a, b) -> {
            if (!a.isFieldExists() || a.getFieldValue() == null) {
                if (!b.isFieldExists() || b.getFieldValue() == null) {
                    return 0;
                }
                return 1;
            }
            if (!b.isFieldExists() || b.getFieldValue() == null) {
                return -1;
            }

            try {
                BigDecimal numA = convertToNumber(a.getFieldValue());
                BigDecimal numB = convertToNumber(b.getFieldValue());
                return numA.compareTo(numB);
            } catch (NumberFormatException e) {
                log.warn("Failed to compare as numbers, falling back to string: {}", e.getMessage());
                return String.valueOf(a.getFieldValue()).compareTo(String.valueOf(b.getFieldValue()));
            }
        };
    }

    
    private BigDecimal convertToNumber(Object value) {
        if (value instanceof BigDecimal) {
            return (BigDecimal) value;
        }
        if (value instanceof Integer || value instanceof Long) {
            return BigDecimal.valueOf(((Number) value).longValue());
        }
        if (value instanceof Double || value instanceof Float) {
            return BigDecimal.valueOf(((Number) value).doubleValue());
        }
        if (value instanceof String) {
            return new BigDecimal((String) value);
        }
        throw new NumberFormatException("Cannot convert to number: " + value.getClass());
    }

    
    private Comparator<SortableValue> createStringComparator() {
        return (a, b) -> {
            if (!a.isFieldExists() || a.getFieldValue() == null) {
                if (!b.isFieldExists() || b.getFieldValue() == null) {
                    return 0;
                }
                return 1;
            }
            if (!b.isFieldExists() || b.getFieldValue() == null) {
                return -1;
            }

            String strA = String.valueOf(a.getFieldValue()).toLowerCase();
            String strB = String.valueOf(b.getFieldValue()).toLowerCase();
            return strA.compareTo(strB);
        };
    }

    
    private Comparator<SortableValue> createBooleanComparator() {
        return (a, b) -> {
            if (!a.isFieldExists() || a.getFieldValue() == null) {
                if (!b.isFieldExists() || b.getFieldValue() == null) {
                    return 0;
                }
                return 1;
            }
            if (!b.isFieldExists() || b.getFieldValue() == null) {
                return -1;
            }

            Boolean boolA = (Boolean) a.getFieldValue();
            Boolean boolB = (Boolean) b.getFieldValue();
            return boolA.compareTo(boolB);
        };
    }

    
    private Comparator<SortableValue> createMixedComparator() {
        return createStringComparator();
    }

    
    private QueryResponse.SortMetadata buildSortMetadata(
            List<SortableValue> values,
            FieldTypeInfo fieldTypeInfo,
            SortOrder sortOrder) {
        return QueryResponse.SortMetadata.builder()
                .sortOrder(sortOrder.getValue())
                .fieldType(fieldTypeInfo.getFieldType().name().toLowerCase())
                .recordsWithMissingField(fieldTypeInfo.getMissingCount())
                .recordsWithNullField(fieldTypeInfo.getNullCount())
                .recordsWithTypeMismatch(fieldTypeInfo.getTypeMismatchCount())
                .warnings(fieldTypeInfo.getWarnings())
                .build();
    }

    
    private QueryResponse buildEmptySortResponse(
            String datasetName,
            String field,
            SortOrder sortOrder) {
        return QueryResponse.builder()
                .success(true)
                .datasetName(datasetName)
                .operation("sort_by")
                .field(field)
                .totalRecords(0)
                .results(Collections.emptyList())
                .sortMetadata(QueryResponse.SortMetadata.builder()
                        .sortOrder(sortOrder.getValue())
                        .fieldType("unknown")
                        .recordsWithMissingField(0)
                        .recordsWithNullField(0)
                        .recordsWithTypeMismatch(0)
                        .warnings(List.of("Dataset is empty"))
                        .build())
                .build();
    }

    
    private void validateSortByRequest(QueryRequest request) {
        if (request == null) {
            throw new InvalidJsonException("Query request cannot be null");
        }

        if (!request.isValid()) {
            throw new InvalidJsonException("Query request is invalid");
        }

        if (!request.isSortByQuery()) {
            throw new InvalidFieldException(
                    request.getSortBy(),
                    request.getDatasetName(),
                    "Sort-by field is required");
        }

        jsonValidator.validateDatasetName(request.getDatasetName());

        try {
            SortOrder.fromString(request.getSortOrder());
        } catch (IllegalArgumentException e) {
            throw new InvalidSortOrderException(
                    request.getSortOrder(),
                    request.getDatasetName(),
                    e.getMessage());
        }
    }

    private void validateQueryRequest(QueryRequest request) {
        if (request == null) {
            throw new InvalidJsonException("Query request cannot be null");
        }

        if (request.getDatasetName() == null || request.getDatasetName().trim().isEmpty()) {
            throw new InvalidJsonException("Query request is invalid");
        }

        if (!request.isGroupByQuery()) {
            throw new InvalidFieldException(
                    request.getGroupBy(),
                    request.getDatasetName(),
                    "Group-by field is required");
        }

        jsonValidator.validateDatasetName(request.getDatasetName());
    }

    private void validateRequest(InsertRecordRequest request) {
        if (request == null) {
            throw new InvalidJsonException("Insert request cannot be null");
        }
        if (!request.isValid()) {
            throw new InvalidJsonException("Insert request is invalid");
        }
    }

    private void validateDatasetName(String datasetName) {
        try {
            jsonValidator.validateDatasetName(datasetName);
        } catch (InvalidJsonException e) {
            throw new DatasetValidationException(
                    "Dataset name validation failed: " + e.getMessage(),
                    e.getValidationErrors());
        }
    }

    private void validateJsonData(Map<String, Object> data) {
        jsonValidator.validateJsonData(data, true);
    }

    private DatasetRecord buildEntityFromRequest(InsertRecordRequest request) {
        Map<String, Object> dataCopy = new HashMap<>(request.getData());
        return DatasetRecord.builder()
                .datasetName(request.getDatasetName())
                .data(dataCopy)
                .isDeleted(false)
                .build();
    }

    private InsertRecordResponse buildSuccessResponse(DatasetRecord record) {
        return InsertRecordResponse.success(
                record.getId(),
                record.getDatasetName(),
                record.getCreatedAt() != null ? record.getCreatedAt() : java.time.OffsetDateTime.now());
    }

    
    @lombok.Data
    @lombok.Builder
    private static class FieldExtractionResult {
        private final boolean fieldExists;
        private final Object fieldValue;

        static FieldExtractionResult exists(Object value) {
            return FieldExtractionResult.builder()
                    .fieldExists(true)
                    .fieldValue(value)
                    .build();
        }

        static FieldExtractionResult notExists() {
            return FieldExtractionResult.builder()
                    .fieldExists(false)
                    .fieldValue(null)
                    .build();
        }
    }

    
    @lombok.Data
    @lombok.Builder
    private static class GroupingResult {
        private final Map<String, List<Map<String, Object>>> groups;
        private final int missingFieldCount;
        private final int nullFieldCount;
    }

    
    @lombok.Data
    @lombok.Builder
    private static class SortableValue {
        private final Map<String, Object> jsonData;
        private final Object fieldValue;
        private final boolean fieldExists;
        private final ValueType valueType;
    }

    
    private enum ValueType {
        NULL, NUMBER, BOOLEAN, STRING, ARRAY, OBJECT, UNKNOWN
    }

    
    private enum FieldType {
        NUMERIC, STRING, BOOLEAN, MIXED
    }

    
    @lombok.Data
    @lombok.Builder
    private static class FieldTypeInfo {
        private final FieldType fieldType;
        private final boolean isMixed;
        private final int missingCount;
        private final int nullCount;
        private final int typeMismatchCount;
        private final List<String> warnings;
    }
}
