package com.freightfox.jsondataset.exception;

import com.freightfox.jsondataset.dto.response.ErrorResponse;
import jakarta.servlet.http.HttpServletRequest;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.validation.FieldError;
import org.springframework.web.HttpMediaTypeNotSupportedException;
import org.springframework.web.HttpRequestMethodNotSupportedException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.MissingServletRequestParameterException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;
import org.springframework.web.servlet.NoHandlerFoundException;
import org.springframework.web.servlet.resource.NoResourceFoundException;

import java.util.List;
import java.util.stream.Collectors;

@RestControllerAdvice
@Slf4j
public class GlobalExceptionHandler {

    
    @ExceptionHandler(InvalidJsonException.class)
    public ResponseEntity<ErrorResponse> handleInvalidJsonException(
            InvalidJsonException ex,
            HttpServletRequest request
    ) {
        log.warn("Invalid JSON error at {}: {}", request.getRequestURI(), ex.getMessage());

        ErrorResponse errorResponse = ErrorResponse.of(
                HttpStatus.BAD_REQUEST.value(),
                HttpStatus.BAD_REQUEST.getReasonPhrase(),
                ex.getMessage(),
                ex.getErrorCode(),
                ex.getValidationErrors(),
                request.getRequestURI()
        );

        return ResponseEntity
                .status(HttpStatus.BAD_REQUEST)
                .body(errorResponse);
    }

    
    @ExceptionHandler(DatasetValidationException.class)
    public ResponseEntity<ErrorResponse> handleDatasetValidationException(
            DatasetValidationException ex,
            HttpServletRequest request
    ) {
        log.warn("Dataset validation error at {}: {}", request.getRequestURI(), ex.getMessage());

        ErrorResponse errorResponse = ErrorResponse.of(
                HttpStatus.BAD_REQUEST.value(),
                HttpStatus.BAD_REQUEST.getReasonPhrase(),
                ex.getMessage(),
                ex.getErrorCode(),
                ex.getValidationErrors(),
                request.getRequestURI()
        );

        return ResponseEntity
                .status(HttpStatus.BAD_REQUEST)
                .body(errorResponse);
    }

    
    @ExceptionHandler(DatasetNotFoundException.class)
    public ResponseEntity<ErrorResponse> handleDatasetNotFoundException(
            DatasetNotFoundException ex,
            HttpServletRequest request
    ) {
        log.warn("Dataset not found at {}: {}", request.getRequestURI(), ex.getMessage());

        ErrorResponse errorResponse = ErrorResponse.of(
                HttpStatus.NOT_FOUND.value(),
                HttpStatus.NOT_FOUND.getReasonPhrase(),
                ex.getMessage(),
                ex.getErrorCode(),
                List.of(String.format("Dataset '%s' does not exist or has no records", ex.getDatasetName())),
                request.getRequestURI()
        );

        return ResponseEntity
                .status(HttpStatus.NOT_FOUND)
                .body(errorResponse);
    }

    
    @ExceptionHandler(InvalidFieldException.class)
    public ResponseEntity<ErrorResponse> handleInvalidFieldException(
            InvalidFieldException ex,
            HttpServletRequest request
    ) {
        log.warn("Invalid field error at {}: {}", request.getRequestURI(), ex.getMessage());

        ErrorResponse errorResponse = ErrorResponse.of(
                HttpStatus.BAD_REQUEST.value(),
                HttpStatus.BAD_REQUEST.getReasonPhrase(),
                ex.getMessage(),
                ex.getErrorCode(),
                List.of(String.format("Field '%s' in dataset '%s' is invalid", 
                        ex.getFieldName(), ex.getDatasetName())),
                request.getRequestURI()
        );

        return ResponseEntity
                .status(HttpStatus.BAD_REQUEST)
                .body(errorResponse);
    }

    
    @ExceptionHandler(InvalidSortOrderException.class)
    public ResponseEntity<ErrorResponse> handleInvalidSortOrderException(
            InvalidSortOrderException ex,
            HttpServletRequest request
    ) {
        log.warn("Invalid sort order error at {}: {}", request.getRequestURI(), ex.getMessage());

        ErrorResponse errorResponse = ErrorResponse.of(
                HttpStatus.BAD_REQUEST.value(),
                HttpStatus.BAD_REQUEST.getReasonPhrase(),
                ex.getMessage(),
                ex.getErrorCode(),
                List.of("Valid sort orders: 'asc' (ascending) or 'desc' (descending)"),
                request.getRequestURI()
        );

        return ResponseEntity
                .status(HttpStatus.BAD_REQUEST)
                .body(errorResponse);
    }

    
    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<ErrorResponse> handleValidationException(
            MethodArgumentNotValidException ex,
            HttpServletRequest request
    ) {
        log.warn("Validation error at {}: {}", request.getRequestURI(), ex.getMessage());

        List<String> errorDetails = ex.getBindingResult()
                .getAllErrors()
                .stream()
                .map(error -> {
                    if (error instanceof FieldError fieldError) {
                        return String.format("%s: %s", 
                                fieldError.getField(), 
                                fieldError.getDefaultMessage());
                    }
                    return error.getDefaultMessage();
                })
                .collect(Collectors.toList());

        ErrorResponse errorResponse = ErrorResponse.of(
                HttpStatus.BAD_REQUEST.value(),
                HttpStatus.BAD_REQUEST.getReasonPhrase(),
                "Request validation failed",
                "VALIDATION_ERROR",
                errorDetails,
                request.getRequestURI()
        );

        return ResponseEntity
                .status(HttpStatus.BAD_REQUEST)
                .body(errorResponse);
    }

    
    @ExceptionHandler(HttpMessageNotReadableException.class)
    public ResponseEntity<ErrorResponse> handleMalformedJsonException(
            HttpMessageNotReadableException ex,
            HttpServletRequest request
    ) {
        log.warn("Malformed JSON at {}: {}", request.getRequestURI(), ex.getMessage());

        String detailMessage = ex.getMostSpecificCause().getMessage();
        
        ErrorResponse errorResponse = ErrorResponse.of(
                HttpStatus.BAD_REQUEST.value(),
                HttpStatus.BAD_REQUEST.getReasonPhrase(),
                "Malformed JSON in request body",
                "MALFORMED_JSON",
                List.of(detailMessage),
                request.getRequestURI()
        );

        return ResponseEntity
                .status(HttpStatus.BAD_REQUEST)
                .body(errorResponse);
    }

    
    @ExceptionHandler(MethodArgumentTypeMismatchException.class)
    public ResponseEntity<ErrorResponse> handleTypeMismatchException(
            MethodArgumentTypeMismatchException ex,
            HttpServletRequest request
    ) {
        log.warn("Type mismatch error at {}: {}", request.getRequestURI(), ex.getMessage());

        String parameterName = ex.getName();
        String providedValue = ex.getValue() != null ? ex.getValue().toString() : "null";
        String requiredType = ex.getRequiredType() != null 
                ? ex.getRequiredType().getSimpleName() 
                : "unknown";

        String message = String.format(
                "Invalid value '%s' for parameter '%s'. Expected type: %s",
                providedValue, parameterName, requiredType
        );

        ErrorResponse errorResponse = ErrorResponse.of(
                HttpStatus.BAD_REQUEST.value(),
                HttpStatus.BAD_REQUEST.getReasonPhrase(),
                message,
                "TYPE_MISMATCH",
                List.of(String.format("Parameter '%s' must be of type %s", parameterName, requiredType)),
                request.getRequestURI()
        );

        return ResponseEntity
                .status(HttpStatus.BAD_REQUEST)
                .body(errorResponse);
    }

    
    @ExceptionHandler(MissingServletRequestParameterException.class)
    public ResponseEntity<ErrorResponse> handleMissingParameterException(
            MissingServletRequestParameterException ex,
            HttpServletRequest request
    ) {
        log.warn("Missing parameter at {}: {}", request.getRequestURI(), ex.getMessage());

        String message = String.format(
                "Missing required parameter '%s' of type %s",
                ex.getParameterName(),
                ex.getParameterType()
        );

        ErrorResponse errorResponse = ErrorResponse.of(
                HttpStatus.BAD_REQUEST.value(),
                HttpStatus.BAD_REQUEST.getReasonPhrase(),
                message,
                "MISSING_PARAMETER",
                List.of(String.format("Parameter '%s' is required", ex.getParameterName())),
                request.getRequestURI()
        );

        return ResponseEntity
                .status(HttpStatus.BAD_REQUEST)
                .body(errorResponse);
    }

    
    @ExceptionHandler(HttpRequestMethodNotSupportedException.class)
    public ResponseEntity<ErrorResponse> handleMethodNotSupportedException(
            HttpRequestMethodNotSupportedException ex,
            HttpServletRequest request
    ) {
        log.warn("Method not supported at {}: {}", request.getRequestURI(), ex.getMessage());

        String supportedMethods = ex.getSupportedHttpMethods() != null
                ? String.join(", ", ex.getSupportedHttpMethods().stream()
                        .map(org.springframework.http.HttpMethod::name)
                        .toList())
                : "Unknown";

        String message = String.format(
                "Method '%s' not allowed for this endpoint",
                ex.getMethod()
        );

        ErrorResponse errorResponse = ErrorResponse.of(
                HttpStatus.METHOD_NOT_ALLOWED.value(),
                HttpStatus.METHOD_NOT_ALLOWED.getReasonPhrase(),
                message,
                "METHOD_NOT_ALLOWED",
                List.of(String.format("Supported methods: %s", supportedMethods)),
                request.getRequestURI()
        );

        return ResponseEntity
                .status(HttpStatus.METHOD_NOT_ALLOWED)
                .body(errorResponse);
    }

    
    @ExceptionHandler(HttpMediaTypeNotSupportedException.class)
    public ResponseEntity<ErrorResponse> handleUnsupportedMediaTypeException(
            HttpMediaTypeNotSupportedException ex,
            HttpServletRequest request
    ) {
        log.warn("Unsupported media type at {}: {}", request.getRequestURI(), ex.getMessage());

        String supportedTypes = ex.getSupportedMediaTypes().stream()
                .map(Object::toString)
                .collect(Collectors.joining(", "));

        String message = String.format(
                "Unsupported media type '%s'",
                ex.getContentType()
        );

        ErrorResponse errorResponse = ErrorResponse.of(
                HttpStatus.UNSUPPORTED_MEDIA_TYPE.value(),
                HttpStatus.UNSUPPORTED_MEDIA_TYPE.getReasonPhrase(),
                message,
                "UNSUPPORTED_MEDIA_TYPE",
                List.of(String.format("Supported types: %s", supportedTypes)),
                request.getRequestURI()
        );

        return ResponseEntity
                .status(HttpStatus.UNSUPPORTED_MEDIA_TYPE)
                .body(errorResponse);
    }

    
    @ExceptionHandler({NoHandlerFoundException.class, NoResourceFoundException.class})
    public ResponseEntity<ErrorResponse> handleNotFoundException(
            Exception ex,
            HttpServletRequest request
    ) {
        log.warn("Endpoint not found: {}", request.getRequestURI());

        ErrorResponse errorResponse = ErrorResponse.of(
                HttpStatus.NOT_FOUND.value(),
                HttpStatus.NOT_FOUND.getReasonPhrase(),
                "The requested endpoint does not exist",
                "ENDPOINT_NOT_FOUND",
                List.of(String.format("No handler found for %s %s", 
                        request.getMethod(), request.getRequestURI())),
                request.getRequestURI()
        );

        return ResponseEntity
                .status(HttpStatus.NOT_FOUND)
                .body(errorResponse);
    }

    
    @ExceptionHandler(Exception.class)
    public ResponseEntity<ErrorResponse> handleGenericException(
            Exception ex,
            HttpServletRequest request
    ) {
        log.error("Unexpected error at {}: ", request.getRequestURI(), ex);

        ErrorResponse errorResponse = ErrorResponse.of(
                HttpStatus.INTERNAL_SERVER_ERROR.value(),
                HttpStatus.INTERNAL_SERVER_ERROR.getReasonPhrase(),
                "An unexpected error occurred. Please try again later or contact support.",
                "INTERNAL_ERROR",
                null,
                request.getRequestURI()
        );

        return ResponseEntity
                .status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(errorResponse);
    }
}
