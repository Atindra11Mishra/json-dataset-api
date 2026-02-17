package com.freightfox.jsondataset.model.enums;

public enum SortOrder {
    ASC("asc"),
    DESC("desc");

    private final String value;

    SortOrder(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    public static SortOrder fromString(String value) {
        if (value == null) {
            return ASC;
        }

        String normalized = value.trim().toLowerCase();

        return switch (normalized) {
            case "asc", "ascending" -> ASC;
            case "desc", "descending" -> DESC;
            default -> throw new IllegalArgumentException(
                    "Invalid sort order: " + value + ". Must be 'asc' or 'desc'");
        };
    }

    public boolean isAscending() {
        return this == ASC;
    }

    public boolean isDescending() {
        return this == DESC;
    }
}
