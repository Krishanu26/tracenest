package com.dataflow.job;

import com.dataflow.model.JobStatus;
import com.dataflow.model.ProcessingResult;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicLong;

/** Thread-safe in-memory representation of a submitted processing job. */
public class ProcessingJob {
    private final String id;
    private final Instant submittedAt = Instant.now();
    private final int partitions;
    private final AtomicLong recordsProcessed = new AtomicLong();
    private volatile JobStatus status = JobStatus.QUEUED;
    private volatile Instant completedAt;
    private volatile long durationMillis;
    private volatile String error;
    private volatile ProcessingResult result;

    public ProcessingJob(String id, int partitions) {
        this.id = id;
        this.partitions = partitions;
    }

    public String getId() { return id; }
    public int getPartitions() { return partitions; }
    public Instant getSubmittedAt() { return submittedAt; }
    public long getRecordsProcessed() { return recordsProcessed.get(); }
    public JobStatus getStatus() { return status; }
    public Instant getCompletedAt() { return completedAt; }
    public long getDurationMillis() { return durationMillis; }
    public String getError() { return error; }
    public ProcessingResult getResult() { return result; }

    public void addProcessed(long count) {
        if (count < 0) {
            throw new IllegalArgumentException("Processed record count cannot be negative");
        }
        recordsProcessed.addAndGet(count);
    }

    /** Only a queued job can transition to running. */
    public synchronized boolean markRunning() {
        if (status != JobStatus.QUEUED) {
            return false;
        }
        status = JobStatus.RUNNING;
        return true;
    }

    /** Terminal transitions are accepted only from RUNNING. */
    public synchronized boolean complete(ProcessingResult completedResult, long elapsedMillis) {
        if (status != JobStatus.RUNNING) {
            return false;
        }
        this.result = completedResult;
        this.durationMillis = Math.max(0, elapsedMillis);
        this.completedAt = Instant.now();
        this.status = JobStatus.COMPLETED;
        return true;
    }

    public synchronized boolean fail(String message, long elapsedMillis) {
        if (status != JobStatus.RUNNING) {
            return false;
        }
        markFailed(message, elapsedMillis);
        return true;
    }

    /** A queued job can fail if shutdown prevents it from starting. */
    public synchronized boolean failBeforeStart(String message) {
        if (status != JobStatus.QUEUED) {
            return false;
        }
        markFailed(message, 0);
        return true;
    }

    private void markFailed(String message, long elapsedMillis) {
        this.error = message == null || message.isBlank() ? "Processing failed" : message;
        this.durationMillis = Math.max(0, elapsedMillis);
        this.completedAt = Instant.now();
        this.status = JobStatus.FAILED;
    }
}
