package com.dataflow.job;

import com.dataflow.model.ErrorEventRequest;
import com.dataflow.model.EventReceipt;
import com.dataflow.model.IssueSnapshot;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Component
public class IssueStore {
    private final int issueRetentionLimit;
    private final int eventIdRetentionLimit;
    private final Map<String, Issue> issues = new LinkedHashMap<>();
    private final Map<EventKey, String> issueByEventId = new LinkedHashMap<>();

    public IssueStore(@Value("${dataflow.events.issue-retention-count:1000}") int issueRetentionLimit) {
        if (issueRetentionLimit < 1 || issueRetentionLimit > 100_000) {
            throw new IllegalArgumentException("Issue retention count must be between 1 and 100000");
        }
        this.issueRetentionLimit = issueRetentionLimit;
        this.eventIdRetentionLimit = Math.max(issueRetentionLimit, 5_000);
    }

    public synchronized EventReceipt ingest(ErrorEventRequest event, String fingerprint) {
        EventKey eventKey = new EventKey(event.project(), event.eventId());
        String existingIssueId = issueByEventId.get(eventKey);
        if (existingIssueId != null) {
            Issue existingIssue = issues.get(existingIssueId);
            return new EventReceipt(event.eventId(), existingIssueId, true, existingIssue.snapshot());
        }

        Instant now = Instant.now();
        Issue issue = issues.get(fingerprint);
        if (issue == null) {
            evictOldestIssueIfFull();
            issue = new Issue(fingerprint, event.project(), event.exceptionType(), event.message(), now);
            issues.put(fingerprint, issue);
        } else {
            issue.record(now, event.message());
        }
        issueByEventId.put(eventKey, fingerprint);
        trimEventIds();
        return new EventReceipt(event.eventId(), fingerprint, false, issue.snapshot());
    }

    public synchronized List<IssueSnapshot> findAll() {
        return issues.values().stream()
                .map(Issue::snapshot)
                .sorted(Comparator.comparing(IssueSnapshot::lastSeen).reversed())
                .toList();
    }

    public synchronized IssueSnapshot find(String issueId) {
        Issue issue = issues.get(issueId);
        return issue == null ? null : issue.snapshot();
    }

    private void evictOldestIssueIfFull() {
        if (issues.size() < issueRetentionLimit) {
            return;
        }
        String oldestIssueId = issues.keySet().iterator().next();
        issues.remove(oldestIssueId);
        issueByEventId.entrySet().removeIf(entry -> entry.getValue().equals(oldestIssueId));
    }

    private void trimEventIds() {
        while (issueByEventId.size() > eventIdRetentionLimit) {
            EventKey oldestEvent = issueByEventId.keySet().iterator().next();
            issueByEventId.remove(oldestEvent);
        }
    }

    private record EventKey(String project, String eventId) {}

    private static final class Issue {
        private final String issueId;
        private final String project;
        private final String exceptionType;
        private String title;
        private long eventCount = 1;
        private final Instant firstSeen;
        private Instant lastSeen;

        private Issue(String issueId, String project, String exceptionType, String title, Instant now) {
            this.issueId = issueId;
            this.project = project;
            this.exceptionType = exceptionType;
            this.title = title;
            this.firstSeen = now;
            this.lastSeen = now;
        }

        private void record(Instant now, String latestTitle) {
            eventCount++;
            lastSeen = now;
            title = latestTitle;
        }

        private IssueSnapshot snapshot() {
            return new IssueSnapshot(issueId, project, exceptionType, title, eventCount, firstSeen, lastSeen);
        }
    }
}
