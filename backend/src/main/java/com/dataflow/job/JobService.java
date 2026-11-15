package com.dataflow.job;

import com.dataflow.engine.CsvDatasetReader;
import com.dataflow.engine.DataFlowEngine;
import com.dataflow.model.ProcessingResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Locale;
import java.util.UUID;
import java.util.concurrent.RejectedExecutionException;

@Service
public class JobService {
    private static final Logger LOGGER = LoggerFactory.getLogger(JobService.class);

    private final JobStore store;
    private final CsvDatasetReader reader;
    private final DataFlowEngine engine;
    private final JobExecutionPool executionPool;

    public JobService(JobStore store, CsvDatasetReader reader,
                      DataFlowEngine engine, JobExecutionPool executionPool) {
        this.store = store;
        this.reader = reader;
        this.engine = engine;
        this.executionPool = executionPool;
    }

    public ProcessingJob submit(MultipartFile file, String column, int partitions) throws IOException {
        validate(file, column, partitions);

        Path temp = Files.createTempFile("dataflow-", ".csv");
        try {
            file.transferTo(temp);
        } catch (IOException | RuntimeException exception) {
            deleteTemporaryFile(temp);
            throw exception;
        }

        ProcessingJob job = new ProcessingJob(UUID.randomUUID().toString(), partitions);
        store.save(job);
        try {
            executionPool.submit(
                    () -> execute(job, temp, column, partitions),
                    () -> {
                        job.failBeforeStart("Application shutdown prevented the job from starting");
                        deleteTemporaryFile(temp);
                        store.pruneTerminalJobs();
                    });
            return job;
        } catch (RejectedExecutionException rejected) {
            store.delete(job.getId());
            deleteTemporaryFile(temp);
            throw new JobQueueFullException(
                    "The processing queue is full. Please retry after a short delay.", rejected);
        }
    }

    private void validate(MultipartFile file, String column, int partitions) {
        if (file == null || file.isEmpty()) {
            throw new IllegalArgumentException("Please upload a non-empty CSV file");
        }
        if (column == null || column.isBlank()) {
            throw new IllegalArgumentException("Column is required");
        }
        if (partitions < 1 || partitions > 128) {
            throw new IllegalArgumentException("Partitions must be between 1 and 128");
        }
        String original = file.getOriginalFilename();
        if (original == null || !original.toLowerCase(Locale.ROOT).endsWith(".csv")) {
            throw new IllegalArgumentException("Only CSV files are supported");
        }
    }

    private void execute(ProcessingJob job, Path file, String column, int partitions) {
        long started = System.nanoTime();
        if (!job.markRunning()) {
            deleteTemporaryFile(file);
            return;
        }

        try {
            CsvDatasetReader.Dataset dataset = reader.read(file);
            if (!dataset.headers().contains(column)) {
                throw new IllegalArgumentException(
                        "Column not found. Available columns: " + dataset.headers());
            }

            job.addProcessed(dataset.rows().size());
            var counts = engine.countByColumn(dataset.rows(), column, partitions);
            long elapsed = elapsedMillis(started);
            ProcessingResult result = new ProcessingResult(
                    job.getId(), "COMPLETED", dataset.rows().size(),
                    Math.min(partitions, Math.max(1, dataset.rows().size())),
                    elapsed, counts, Instant.now());
            job.complete(result, elapsed);
        } catch (Exception exception) {
            job.fail(safeMessage(exception), elapsedMillis(started));
        } finally {
            deleteTemporaryFile(file);
            store.pruneTerminalJobs();
        }
    }

    private long elapsedMillis(long started) {
        return (System.nanoTime() - started) / 1_000_000;
    }

    private String safeMessage(Exception exception) {
        String message = exception.getMessage();
        return message == null || message.isBlank() ? "Processing failed" : message;
    }

    private void deleteTemporaryFile(Path file) {
        try {
            Files.deleteIfExists(file);
        } catch (IOException exception) {
            LOGGER.warn("Could not delete temporary upload file {}", file, exception);
        }
    }
}
