package com.dataflow.job;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class JobExecutionPoolTest {
    @Test
    void rejectsSubmissionWhenWorkersAndQueueAreFull() throws Exception {
        JobExecutionPool pool = new JobExecutionPool(1, 1);
        CountDownLatch workerStarted = new CountDownLatch(1);
        CountDownLatch releaseWorker = new CountDownLatch(1);
        try {
            pool.submit(() -> {
                workerStarted.countDown();
                try {
                    releaseWorker.await(5, TimeUnit.SECONDS);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                }
            }, () -> { });

            assertTrue(workerStarted.await(2, TimeUnit.SECONDS));
            pool.submit(() -> { }, () -> { });
            assertEquals(1, pool.queuedCount());
            assertThrows(RejectedExecutionException.class, () -> pool.submit(() -> { }, () -> { }));
        } finally {
            releaseWorker.countDown();
            pool.close();
        }
    }

    @Test
    void failsAndCleansUpQueuedJobWhenShutdownCannotDrainQueue() throws Exception {
        JobExecutionPool pool = new JobExecutionPool(1, 1, 1, TimeUnit.MILLISECONDS);
        CountDownLatch workerStarted = new CountDownLatch(1);
        CountDownLatch workerInterrupted = new CountDownLatch(1);
        ProcessingJob queuedJob = new ProcessingJob("queued-job", 1);
        try {
            pool.submit(() -> {
                workerStarted.countDown();
                try {
                    new CountDownLatch(1).await();
                } catch (InterruptedException interrupted) {
                    workerInterrupted.countDown();
                    Thread.currentThread().interrupt();
                }
            }, () -> { });
            assertTrue(workerStarted.await(2, TimeUnit.SECONDS));

            pool.submit(() -> { }, () -> queuedJob.failBeforeStart("Application is shutting down"));
            pool.close();

            assertTrue(workerInterrupted.await(2, TimeUnit.SECONDS));
            assertEquals(com.dataflow.model.JobStatus.FAILED, queuedJob.getStatus());
            assertEquals("Application is shutting down", queuedJob.getError());
            assertNotNull(queuedJob.getCompletedAt());
        } finally {
            pool.close();
        }
    }

    @Test
    void rejectsInvalidPoolConfiguration() {
        assertThrows(IllegalArgumentException.class, () -> new JobExecutionPool(0, 1));
        assertThrows(IllegalArgumentException.class, () -> new JobExecutionPool(1, 0));
    }
}
