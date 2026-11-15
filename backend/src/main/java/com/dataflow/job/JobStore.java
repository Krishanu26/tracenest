package com.dataflow.job;

import com.dataflow.model.JobStatus;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class JobStore {
    private final ConcurrentHashMap<String, ProcessingJob> jobs = new ConcurrentHashMap<>();
    private final int terminalRetentionCount;

    public JobStore(@Value("${dataflow.jobs.terminal-retention-count:1000}") int terminalRetentionCount) {
        if (terminalRetentionCount < 1) {
            throw new IllegalArgumentException("Terminal job retention count must be at least 1");
        }
        this.terminalRetentionCount = terminalRetentionCount;
    }

    public void save(ProcessingJob job) {
        jobs.put(job.getId(), job);
    }

    public Optional<ProcessingJob> find(String id) {
        return Optional.ofNullable(jobs.get(id));
    }

    public void delete(String id) {
        jobs.remove(id);
    }

    public Collection<ProcessingJob> findAll() {
        return jobs.values().stream()
                .sorted(Comparator.comparing(ProcessingJob::getSubmittedAt).reversed())
                .toList();
    }

    public void pruneTerminalJobs() {
        List<ProcessingJob> terminalJobs = jobs.values().stream()
                .filter(job -> job.getStatus() == JobStatus.COMPLETED || job.getStatus() == JobStatus.FAILED)
                .sorted(Comparator.comparing(ProcessingJob::getCompletedAt))
                .toList();
        int excess = terminalJobs.size() - terminalRetentionCount;
        for (int i = 0; i < excess; i++) {
            ProcessingJob job = terminalJobs.get(i);
            jobs.remove(job.getId(), job);
        }
    }
}
