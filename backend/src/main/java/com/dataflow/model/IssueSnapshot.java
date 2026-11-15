package com.dataflow.model;

import java.time.Instant;

public record IssueSnapshot(
        String issueId,
        String project,
        String exceptionType,
        String title,
        long eventCount,
        Instant firstSeen,
        Instant lastSeen
) {}
