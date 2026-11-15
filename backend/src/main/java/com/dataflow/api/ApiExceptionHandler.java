package com.dataflow.api;

import com.dataflow.job.JobQueueFullException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.server.ResponseStatusException;

import java.time.Instant;
import java.util.Map;

@RestControllerAdvice
public class ApiExceptionHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(ApiExceptionHandler.class);

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<Map<String, Object>> badRequest(IllegalArgumentException exception) {
        return ResponseEntity.badRequest().body(errorBody(400, exception.getMessage()));
    }

    @ExceptionHandler(HttpMessageNotReadableException.class)
    public ResponseEntity<Map<String, Object>> malformedRequest(HttpMessageNotReadableException exception) {
        return ResponseEntity.badRequest().body(errorBody(400, "Request body is missing or malformed"));
    }

    @ExceptionHandler(JobQueueFullException.class)
    public ResponseEntity<Map<String, Object>> queueFull(JobQueueFullException exception) {
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                .header(HttpHeaders.RETRY_AFTER, "5")
                .body(errorBody(503, exception.getMessage()));
    }

    @ExceptionHandler(ResponseStatusException.class)
    public ResponseEntity<Map<String, Object>> statusException(ResponseStatusException exception) {
        int status = exception.getStatusCode().value();
        return ResponseEntity.status(exception.getStatusCode())
                .body(errorBody(status, exception.getReason()));
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<Map<String, Object>> internalError(Exception exception) {
        LOGGER.error("Unexpected API error", exception);
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(errorBody(500, "Unexpected server error"));
    }

    private Map<String, Object> errorBody(int status, String message) {
        return Map.of("timestamp", Instant.now(), "status", status, "error",
                message == null || message.isBlank() ? "Request failed" : message);
    }
}
