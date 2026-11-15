package com.dataflow.api;

import com.dataflow.job.JobService;
import com.dataflow.job.JobStore;
import com.dataflow.job.ProcessingJob;
import com.dataflow.model.JobSnapshot;
import com.dataflow.model.ProcessingResult;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.server.ResponseStatusException;
import java.io.IOException;
import java.util.List;

@RestController
@RequestMapping("/api/jobs")
public class JobController {
    private final JobService service;
    private final JobStore store;
    public JobController(JobService service, JobStore store) { this.service = service; this.store = store; }

    @PostMapping(consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    @ResponseStatus(HttpStatus.ACCEPTED)
    public JobSnapshot submit(@RequestParam("file") MultipartFile file,
                              @RequestParam("column") String column,
                              @RequestParam(defaultValue = "4") int partitions) throws IOException {
        return snapshot(service.submit(file, column, partitions));
    }

    @GetMapping("/{id}")
    public JobSnapshot status(@PathVariable("id") String id) { return snapshot(find(id)); }

    @GetMapping("/{id}/result")
    public ProcessingResult result(@PathVariable("id") String id) {
        ProcessingJob job = find(id);
        if (job.getResult() == null) throw new ResponseStatusException(HttpStatus.CONFLICT, "Job has not completed successfully");
        return job.getResult();
    }

    @GetMapping
    public List<JobSnapshot> list() { return store.findAll().stream().map(this::snapshot).toList(); }

    private ProcessingJob find(String id) {
        return store.find(id).orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Job not found"));
    }
    private JobSnapshot snapshot(ProcessingJob job) {
        return new JobSnapshot(job.getId(), job.getStatus(), job.getRecordsProcessed(), job.getPartitions(),
                job.getDurationMillis(), job.getError(), job.getSubmittedAt(), job.getCompletedAt());
    }
}
