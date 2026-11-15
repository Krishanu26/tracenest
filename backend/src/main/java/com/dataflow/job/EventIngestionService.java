package com.dataflow.job;

import com.dataflow.model.ErrorEventRequest;
import com.dataflow.model.EventReceipt;
import com.dataflow.model.IssueSnapshot;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.List;
import java.util.Locale;
import java.util.regex.Pattern;

@Service
public class EventIngestionService {
    private static final Pattern UUID_PATTERN = Pattern.compile(
            "(?i)\\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\\b");
    private static final Pattern NUMBER_PATTERN = Pattern.compile("\\b\\d+\\b");
    private static final int MAX_EVENT_ID_LENGTH = 64;
    private static final int MAX_PROJECT_LENGTH = 100;
    private static final int MAX_EXCEPTION_TYPE_LENGTH = 200;
    private static final int MAX_MESSAGE_LENGTH = 2_000;
    private static final int MAX_STACK_TRACE_LENGTH = 12_000;

    private final IssueStore store;

    public EventIngestionService(IssueStore store) {
        this.store = store;
    }

    public EventReceipt ingest(ErrorEventRequest request) {
        ErrorEventRequest event = validateAndNormalize(request);
        return store.ingest(event, fingerprint(event));
    }

    public List<IssueSnapshot> findIssues() {
        return store.findAll();
    }

    public IssueSnapshot findIssue(String issueId) {
        return store.find(issueId);
    }

    private ErrorEventRequest validateAndNormalize(ErrorEventRequest request) {
        if (request == null) {
            throw new IllegalArgumentException("Event body is required");
        }
        String eventId = required(request.eventId(), "eventId", MAX_EVENT_ID_LENGTH);
        String project = required(request.project(), "project", MAX_PROJECT_LENGTH);
        String exceptionType = required(request.exceptionType(), "exceptionType", MAX_EXCEPTION_TYPE_LENGTH);
        String message = required(request.message(), "message", MAX_MESSAGE_LENGTH);
        if (request.stackTrace() != null && request.stackTrace().length() > MAX_STACK_TRACE_LENGTH) {
            throw new IllegalArgumentException("stackTrace must be at most 12000 characters");
        }
        return new ErrorEventRequest(eventId, project, exceptionType, message, request.stackTrace());
    }

    private String required(String value, String field, int maxLength) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(field + " is required");
        }
        String normalized = value.trim();
        if (normalized.length() > maxLength) {
            throw new IllegalArgumentException(field + " must be at most " + maxLength + " characters");
        }
        return normalized;
    }

    private String fingerprint(ErrorEventRequest event) {
        String normalizedMessage = UUID_PATTERN.matcher(event.message()).replaceAll("<uuid>");
        normalizedMessage = NUMBER_PATTERN.matcher(normalizedMessage).replaceAll("<number>");
        String groupingKey = fingerprintPart(event.project())
                + fingerprintPart(event.exceptionType())
                + fingerprintPart(normalizedMessage.toLowerCase(Locale.ROOT));
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256")
                    .digest(groupingKey.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(digest);
        } catch (NoSuchAlgorithmException exception) {
            throw new IllegalStateException("SHA-256 is unavailable", exception);
        }
    }

    private String fingerprintPart(String value) {
        return value.length() + ":" + value;
    }
}
