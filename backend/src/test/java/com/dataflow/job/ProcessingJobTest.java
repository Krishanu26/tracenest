package com.dataflow.job;

import com.dataflow.model.JobStatus;
import com.dataflow.model.ProcessingResult;

import java.time.Instant;
import java.util.Map;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class ProcessingJobTest {
    @Test
    void permitsOnlyQueuedToRunningToFailedTransition() {
        ProcessingJob job = new ProcessingJob("job-1", 2);

        assertEquals(JobStatus.QUEUED, job.getStatus());
        assertTrue(job.markRunning());
        assertFalse(job.markRunning());
        assertEquals(JobStatus.RUNNING, job.getStatus());

        assertTrue(job.fail("worker failed", 12));
        assertFalse(job.complete(null, 13));
        assertFalse(job.fail("second failure", 14));
        assertEquals(JobStatus.FAILED, job.getStatus());
        assertEquals("worker failed", job.getError());
        assertNotNull(job.getCompletedAt());
    }

    @Test
    void permitsSuccessfulCompletionOnlyOnce() {
        ProcessingJob job = new ProcessingJob("job-3", 1);
        ProcessingResult result = new ProcessingResult(
                "job-3", "COMPLETED", 2, 1, 4, Map.of("A", 2L), Instant.now());

        assertTrue(job.markRunning());
        assertTrue(job.complete(result, 4));
        assertFalse(job.complete(result, 5));
        assertEquals(JobStatus.COMPLETED, job.getStatus());
        assertSame(result, job.getResult());
        assertNotNull(job.getCompletedAt());
    }

    @Test
    void doesNotCompleteAJobThatHasNotStarted() {
        ProcessingJob job = new ProcessingJob("job-2", 1);
        assertFalse(job.fail("failure", 1));
        assertEquals(JobStatus.QUEUED, job.getStatus());
    }
}
