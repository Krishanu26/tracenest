package com.dataflow.model;

import java.time.Instant;

public record JobSnapshot(String jobId, JobStatus status, long recordsProcessed,
                          int partitions, long durationMillis, String error,
                          Instant submittedAt, Instant completedAt) {}
