package com.dataflow.engine;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

@Component
public class DataFlowEngine implements AutoCloseable {
    private final Partitioner partitioner;
    private final ThreadPoolExecutor workers;

    @Autowired
    public DataFlowEngine(Partitioner partitioner,
                          @Value("${dataflow.engine.worker-count:8}") int configuredWorkers,
                          @Value("${dataflow.engine.queue-capacity:512}") int queueCapacity) {
        if (configuredWorkers < 1 || queueCapacity < 1) {
            throw new IllegalArgumentException("Engine worker count and queue capacity must be positive");
        }
        this.partitioner = partitioner;
        int workerCount = Math.min(8, configuredWorkers);
        AtomicInteger threadNumber = new AtomicInteger();
        this.workers = new ThreadPoolExecutor(
                workerCount,
                workerCount,
                0L,
                TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(queueCapacity),
                runnable -> new Thread(runnable, "dataflow-partition-" + threadNumber.incrementAndGet()),
                (task, executor) -> {
                    while (!executor.isShutdown()) {
                        try {
                            if (executor.getQueue().offer(task, 100, TimeUnit.MILLISECONDS)) {
                                if (!executor.isShutdown() || !executor.remove(task)) {
                                    return;
                                }
                            }
                        } catch (InterruptedException interrupted) {
                            Thread.currentThread().interrupt();
                            throw new RejectedExecutionException(
                                    "Interrupted while waiting for engine capacity", interrupted);
                        }
                    }
                    throw new RejectedExecutionException("DataFlow engine is shutting down");
                }
        );
    }

    public DataFlowEngine(Partitioner partitioner) {
        this(partitioner, Math.max(2, Runtime.getRuntime().availableProcessors()), 512);
    }

    public Map<String, Long> countByColumn(List<Map<String, String>> rows,
                                           String column, int partitionCount) {
        List<List<Map<String, String>>> partitions = partitioner.partition(rows, partitionCount);
        List<CompletableFuture<Map<String, Long>>> tasks = partitions.stream()
                .map(partition -> submitPartition(partition, column))
                .toList();

        Map<String, Long> combined = new HashMap<>();
        for (CompletableFuture<Map<String, Long>> task : tasks) {
            task.join().forEach((value, count) -> combined.merge(value, count, Long::sum));
        }
        return Map.copyOf(combined);
    }

    private CompletableFuture<Map<String, Long>> submitPartition(
            List<Map<String, String>> partition, String column) {
        CompletableFuture<Map<String, Long>> result = new CompletableFuture<>();
        PartitionTask task = new PartitionTask(() -> countPartition(partition, column), result);
        try {
            workers.execute(task);
        } catch (RejectedExecutionException rejected) {
            result.completeExceptionally(rejected);
        }
        return result;
    }

    private Map<String, Long> countPartition(List<Map<String, String>> rows, String column) {
        Map<String, Long> counts = new HashMap<>();
        for (Map<String, String> row : rows) {
            String value = row.getOrDefault(column, "");
            counts.merge(value, 1L, Long::sum);
        }
        return counts;
    }

    @Override
    public void close() {
        workers.shutdown();
        try {
            if (!workers.awaitTermination(30, TimeUnit.SECONDS)) {
                completeAbandonedTasks(workers.shutdownNow());
            }
        } catch (InterruptedException interrupted) {
            completeAbandonedTasks(workers.shutdownNow());
            Thread.currentThread().interrupt();
        }
    }

    private void completeAbandonedTasks(List<Runnable> tasks) {
        for (Runnable task : tasks) {
            if (task instanceof PartitionTask partitionTask) {
                partitionTask.failOnShutdown();
            }
        }
    }

    private static final class PartitionTask implements Runnable {
        private final Supplier<Map<String, Long>> action;
        private final CompletableFuture<Map<String, Long>> result;

        private PartitionTask(Supplier<Map<String, Long>> action, CompletableFuture<Map<String, Long>> result) {
            this.action = action;
            this.result = result;
        }

        @Override
        public void run() {
            try {
                result.complete(action.get());
            } catch (RuntimeException exception) {
                result.completeExceptionally(exception);
            }
        }

        private void failOnShutdown() {
            result.completeExceptionally(new RejectedExecutionException(
                    "DataFlow engine shut down before the partition could run"));
        }
    }
}
