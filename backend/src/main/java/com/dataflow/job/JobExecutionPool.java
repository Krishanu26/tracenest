package com.dataflow.job;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.List;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Owns the application-level queue for submitted jobs.
 * A bounded queue prevents unlimited submissions from consuming memory.
 */
@Component
public class JobExecutionPool implements AutoCloseable {
    private final ThreadPoolExecutor executor;
    private final long shutdownTimeout;
    private final TimeUnit shutdownTimeoutUnit;

    @Autowired
    public JobExecutionPool(
            @Value("${dataflow.jobs.worker-count:4}") int workerCount,
            @Value("${dataflow.jobs.queue-capacity:32}") int queueCapacity) {
        this(workerCount, queueCapacity, 30, TimeUnit.SECONDS);
    }

    JobExecutionPool(int workerCount, int queueCapacity, long shutdownTimeout, TimeUnit shutdownTimeoutUnit) {
        if (workerCount < 1) {
            throw new IllegalArgumentException("Job worker count must be at least 1");
        }
        if (queueCapacity < 1) {
            throw new IllegalArgumentException("Job queue capacity must be at least 1");
        }
        if (shutdownTimeout < 1) {
            throw new IllegalArgumentException("Shutdown timeout must be at least 1");
        }

        this.shutdownTimeout = shutdownTimeout;
        this.shutdownTimeoutUnit = shutdownTimeoutUnit;
        AtomicInteger threadNumber = new AtomicInteger();
        this.executor = new ThreadPoolExecutor(
                workerCount,
                workerCount,
                0L,
                TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(queueCapacity),
                runnable -> {
                    Thread thread = new Thread(runnable,
                            "dataflow-job-" + threadNumber.incrementAndGet());
                    thread.setDaemon(false);
                    return thread;
                },
                new ThreadPoolExecutor.AbortPolicy()
        );
    }

    public void submit(Runnable task, Runnable onAbandoned) {
        executor.execute(new ManagedTask(task, onAbandoned));
    }

    public int activeCount() {
        return executor.getActiveCount();
    }

    public int queuedCount() {
        return executor.getQueue().size();
    }

    public int remainingQueueCapacity() {
        return executor.getQueue().remainingCapacity();
    }

    @Override
    public void close() {
        executor.shutdown();
        try {
            if (!executor.awaitTermination(shutdownTimeout, shutdownTimeoutUnit)) {
                abandonQueuedTasks(executor.shutdownNow());
            }
        } catch (InterruptedException interrupted) {
            abandonQueuedTasks(executor.shutdownNow());
            Thread.currentThread().interrupt();
        }
    }

    private void abandonQueuedTasks(List<Runnable> tasks) {
        for (Runnable task : tasks) {
            if (task instanceof ManagedTask managedTask) {
                managedTask.abandon();
            }
        }
    }

    private record ManagedTask(Runnable action, Runnable onAbandoned) implements Runnable {
        @Override
        public void run() {
            action.run();
        }

        private void abandon() {
            onAbandoned.run();
        }
    }
}
