package com.dataflow.job;

import com.dataflow.model.ErrorEventRequest;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class EventIngestionServiceTest {
    @Test
    void groupsVariableIdentifiersAndSeparatesProjects() {
        EventIngestionService service = new EventIngestionService(new IssueStore(10));

        var first = service.ingest(event("event-1", "shop", "Timeout for order 123"));
        var grouped = service.ingest(event("event-2", "shop", "Timeout for order 987"));
        var otherProject = service.ingest(event("event-3", "billing", "Timeout for order 987"));

        assertEquals(first.issueId(), grouped.issueId());
        assertEquals(2, grouped.issue().eventCount());
        assertNotEquals(first.issueId(), otherProject.issueId());
    }

    @Test
    void repeatedEventIdIsIdempotent() {
        EventIngestionService service = new EventIngestionService(new IssueStore(10));

        service.ingest(event("event-retry", "shop", "Connection failed"));
        var retry = service.ingest(event("event-retry", "shop", "Connection failed"));

        assertTrue(retry.duplicate());
        assertEquals(1, retry.issue().eventCount());
    }

    @Test
    void eventIdsAreScopedToTheirProject() {
        EventIngestionService service = new EventIngestionService(new IssueStore(10));

        var firstProject = service.ingest(event("same-id", "shop", "Connection failed"));
        var secondProject = service.ingest(event("same-id", "billing", "Connection failed"));

        assertFalse(secondProject.duplicate());
        assertNotEquals(firstProject.issueId(), secondProject.issueId());
    }

    @Test
    void fingerprintSeparatesFieldsContainingNewlines() {
        EventIngestionService service = new EventIngestionService(new IssueStore(10));

        var first = service.ingest(new ErrorEventRequest("first", "a\nb", "c", "d", null));
        var second = service.ingest(new ErrorEventRequest("second", "a", "b\nc", "d", null));

        assertNotEquals(first.issueId(), second.issueId());
    }

    @Test
    void rejectsBlankAndOversizedFields() {
        EventIngestionService service = new EventIngestionService(new IssueStore(10));

        assertThrows(IllegalArgumentException.class,
                () -> service.ingest(new ErrorEventRequest("id", "shop", "Error", " ", null)));
        assertThrows(IllegalArgumentException.class,
                () -> service.ingest(new ErrorEventRequest("id", "shop", "Error", "x".repeat(2_001), null)));
    }

    @Test
    void boundsIssueRetention() {
        EventIngestionService service = new EventIngestionService(new IssueStore(1));

        var first = service.ingest(event("first", "shop", "First error"));
        service.ingest(event("second", "shop", "Second error"));

        assertNull(service.findIssue(first.issueId()));
        assertEquals(1, service.findIssues().size());
    }

    @Test
    void groupsConcurrentEventsWithoutLosingCounts() throws Exception {
        EventIngestionService service = new EventIngestionService(new IssueStore(10));
        int eventCount = 100;
        var executor = Executors.newFixedThreadPool(8);
        CountDownLatch ready = new CountDownLatch(8);
        CountDownLatch start = new CountDownLatch(1);
        List<java.util.concurrent.Future<?>> submissions = new ArrayList<>();
        try {
            for (int i = 0; i < eventCount; i++) {
                int eventNumber = i;
                submissions.add(executor.submit(() -> {
                    ready.countDown();
                    start.await();
                    service.ingest(event("concurrent-" + eventNumber, "shop", "Connection failed"));
                    return null;
                }));
            }
            assertTrue(ready.await(5, TimeUnit.SECONDS));
            start.countDown();
            for (var submission : submissions) {
                submission.get(5, TimeUnit.SECONDS);
            }
        } finally {
            executor.shutdownNow();
        }

        assertEquals(100, service.findIssues().get(0).eventCount());
    }

    private ErrorEventRequest event(String eventId, String project, String message) {
        return new ErrorEventRequest(eventId, project, "ConnectionError", message, null);
    }
}
