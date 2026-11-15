package com.dataflow.model;

public record EventReceipt(
        String eventId,
        String issueId,
        boolean duplicate,
        IssueSnapshot issue
) {}
