package com.dataflow.job;

import com.dataflow.model.JobStatus;
import com.dataflow.model.ProcessingResult;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class JobStoreTest {
    @Test
    void retainsNewestTerminalJobsAndNeverEvictsActiveJobs() throws InterruptedException {
        JobStore store = new JobStore(1);
        ProcessingJob oldest = completedJob("oldest");
        store.save(oldest);
        Thread.sleep(2);
        ProcessingJob newest = completedJob("newest");
        store.save(newest);
        ProcessingJob active = new ProcessingJob("active", 1);
        assertTrue(active.markRunning());
        store.save(active);

        store.pruneTerminalJobs();

        assertTrue(store.find(oldest.getId()).isEmpty());
        assertEquals(JobStatus.COMPLETED, store.find(newest.getId()).orElseThrow().getStatus());
        assertEquals(JobStatus.RUNNING, store.find(active.getId()).orElseThrow().getStatus());
        assertEquals(2, store.findAll().size());
    }

    @Test
    void rejectsNonPositiveRetentionLimit() {
        assertThrows(IllegalArgumentException.class, () -> new JobStore(0));
    }

    private ProcessingJob completedJob(String id) {
        ProcessingJob job = new ProcessingJob(id, 1);
        assertTrue(job.markRunning());
        assertTrue(job.complete(
                new ProcessingResult(id, "COMPLETED", 0, 1, 0, Map.of(), Instant.now()), 0));
        return job;
    }
}
