package com.dataflow.model;

import java.time.Instant;
import java.util.Map;

public record ProcessingResult(
        String jobId,
        String status,
        long totalRecords,
        int partitions,
        long durationMillis,
        Map<String, Long> countsByValue,
        Instant completedAt
) {}
