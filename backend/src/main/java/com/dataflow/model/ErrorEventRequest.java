package com.dataflow.model;

public record ErrorEventRequest(
        String eventId,
        String project,
        String exceptionType,
        String message,
        String stackTrace
) {}
