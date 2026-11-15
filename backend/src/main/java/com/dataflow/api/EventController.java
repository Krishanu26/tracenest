package com.dataflow.api;

import com.dataflow.job.EventIngestionService;
import com.dataflow.model.ErrorEventRequest;
import com.dataflow.model.EventReceipt;
import com.dataflow.model.IssueSnapshot;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import java.util.List;

@RestController
public class EventController {
    private final EventIngestionService service;

    public EventController(EventIngestionService service) {
        this.service = service;
    }

    @PostMapping("/api/events")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public EventReceipt ingest(@RequestBody ErrorEventRequest request) {
        return service.ingest(request);
    }

    @GetMapping("/api/issues")
    public List<IssueSnapshot> issues() {
        return service.findIssues();
    }

    @GetMapping("/api/issues/{issueId}")
    public IssueSnapshot issue(@PathVariable("issueId") String issueId) {
        IssueSnapshot issue = service.findIssue(issueId);
        if (issue == null) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Issue not found");
        }
        return issue;
    }
}
