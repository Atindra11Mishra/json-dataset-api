package com.freightfox.jsondataset.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.freightfox.jsondataset.exception.InvalidJsonException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Component
@RequiredArgsConstructor
@Slf4j
public class JsonValidator {

    private final ObjectMapper objectMapper;

    
    public void validateJsonData(Map<String, Object> data, boolean allowEmpty) {
        List<String> errors = new ArrayList<>();

        if (data == null) {
            throw new InvalidJsonException("JSON data cannot be null");
        }

        if (!allowEmpty && data.isEmpty()) {
            errors.add("JSON data cannot be empty");
        }

        for (String key : data.keySet()) {
            if (key == null || key.trim().isEmpty()) {
                errors.add("JSON keys cannot be null or blank");
            }
        }

        try {
            String jsonString = objectMapper.writeValueAsString(data);
            log.debug("JSON validation successful. Serialized length: {} chars", jsonString.length());
        } catch (Exception e) {
            log.error("JSON serialization failed", e);
            errors.add("JSON data is not serializable: " + e.getMessage());
        }

        if (!errors.isEmpty()) {
            throw new InvalidJsonException(
                    "JSON validation failed: " + String.join("; ", errors),
                    errors);
        }
    }

    
    public void validateJsonData(Map<String, Object> data) {
        validateJsonData(data, false);
    }

    
    public void validateDatasetName(String datasetName) {
        List<String> errors = new ArrayList<>();

        if (datasetName == null || datasetName.trim().isEmpty()) {
            throw new InvalidJsonException("Dataset name cannot be null or blank");
        }

        if (datasetName.length() > 255) {
            errors.add("Dataset name exceeds maximum length of 255 characters");
        }

        if (!datasetName.matches("^[a-zA-Z_][a-zA-Z0-9_-]*$")) {
            errors.add("Dataset name must start with letter or underscore and contain only alphanumeric characters, underscores, or hyphens");
        }

        if (!errors.isEmpty()) {
            throw new InvalidJsonException(
                    "Dataset name validation failed: " + String.join("; ", errors),
                    errors);
        }
    }

    
    public boolean isEffectivelyEmpty(Map<String, Object> data) {
        if (data == null || data.isEmpty()) {
            return true;
        }

        return data.values().stream().allMatch(value -> value == null);
    }
}
