package com.freightfox.jsondataset.dto.response;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.*;

import java.time.LocalDateTime;
import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@ToString
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ErrorResponse {

    
    @JsonProperty("timestamp")
    @JsonFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss")
    @Builder.Default
    private LocalDateTime timestamp = LocalDateTime.now();

    
    @JsonProperty("status")
    private Integer status;

    
    @JsonProperty("error")
    private String error;

    
    @JsonProperty("message")
    private String message;

    
    @JsonProperty("details")
    private List<String> details;

    
    @JsonProperty("path")
    private String path;

    
    @JsonProperty("code")
    private String code;

    
    public static ErrorResponse of(
            Integer status,
            String error,
            String message,
            String path
    ) {
        return ErrorResponse.builder()
                .timestamp(LocalDateTime.now())
                .status(status)
                .error(error)
                .message(message)
                .path(path)
                .build();
    }

    
    public static ErrorResponse of(
            Integer status,
            String error,
            String message,
            List<String> details,
            String path
    ) {
        return ErrorResponse.builder()
                .timestamp(LocalDateTime.now())
                .status(status)
                .error(error)
                .message(message)
                .details(details)
                .path(path)
                .build();
    }

    
    public static ErrorResponse of(
            Integer status,
            String error,
            String message,
            String code,
            List<String> details,
            String path
    ) {
        return ErrorResponse.builder()
                .timestamp(LocalDateTime.now())
                .status(status)
                .error(error)
                .message(message)
                .code(code)
                .details(details)
                .path(path)
                .build();
    }
}