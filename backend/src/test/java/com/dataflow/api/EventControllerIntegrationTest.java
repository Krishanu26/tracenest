package com.dataflow.api;

import com.dataflow.model.ErrorEventRequest;
import com.dataflow.model.EventReceipt;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class EventControllerIntegrationTest {
    @Autowired
    private TestRestTemplate client;

    @Test
    void acceptsEventsGroupsIssuesAndDeduplicatesRetries() {
        String project = "integration-" + UUID.randomUUID();
        ErrorEventRequest event = new ErrorEventRequest(
                UUID.randomUUID().toString(), project, "ValueError", "Order 123 failed", null);

        ResponseEntity<EventReceipt> firstResponse =
                client.postForEntity("/api/events", event, EventReceipt.class);
        assertEquals(HttpStatus.ACCEPTED, firstResponse.getStatusCode());
        EventReceipt first = firstResponse.getBody();
        assertNotNull(first);
        assertFalse(first.duplicate());

        ResponseEntity<EventReceipt> retryResponse =
                client.postForEntity("/api/events", event, EventReceipt.class);
        assertEquals(HttpStatus.ACCEPTED, retryResponse.getStatusCode());
        EventReceipt retry = retryResponse.getBody();
        assertNotNull(retry);
        assertTrue(retry.duplicate());
        assertEquals(first.issueId(), retry.issueId());

        ResponseEntity<String> issueResponse =
                client.getForEntity("/api/issues/" + first.issueId(), String.class);
        assertEquals(HttpStatus.OK, issueResponse.getStatusCode(), issueResponse.getBody());
        assertNotNull(issueResponse.getBody());
        assertTrue(issueResponse.getBody().contains("\"eventCount\":1"));
    }
}
