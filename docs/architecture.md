# DataFlow architecture notes

## Scope and terminology

DataFlow is a **single-process parallel data processing engine**. A submitted job is handled by one application instance. Within that process, CSV records are partitioned and processed concurrently by a local worker pool. The project does not currently coordinate work across machines.

## Components

### API layer

`JobController` exposes job submission, status, listing, and result retrieval. `ApiExceptionHandler` translates invalid input and queue saturation into HTTP responses.

### Application/job layer

`JobService` validates requests, copies the uploaded CSV to a temporary file, registers a job, and submits it to `JobExecutionPool`. It owns the high-level job workflow and temporary-file cleanup.

`ProcessingJob` stores the job's in-memory state. Its synchronized transition methods enforce the lifecycle:

```text
QUEUED -> RUNNING -> COMPLETED
                   -> FAILED
   \--------------> FAILED (forced shutdown before start)
```

Only a queued job can start. Running jobs can complete or fail; queued jobs can fail only when forced shutdown prevents them from starting.

`JobStore` is a process-local `ConcurrentHashMap`. It retains at most 1,000 terminal jobs by default, pruning the oldest completed or failed jobs when processing finishes. The limit is configurable using `DATAFLOW_JOB_TERMINAL_RETENTION_COUNT`. Queued and running jobs are never evicted. State is not durable and all jobs disappear when the process restarts.

### Admission control

`JobExecutionPool` uses a fixed-size `ThreadPoolExecutor` with an `ArrayBlockingQueue` and `AbortPolicy`. This creates an explicit upper bound on active and waiting submitted jobs. When saturated, submission throws `RejectedExecutionException`; `JobService` removes the unaccepted job record, deletes its temporary file, and reports queue saturation as HTTP 503 with `Retry-After: 5`.

Defaults are four active job workers and 32 waiting jobs. They are configurable using `DATAFLOW_JOB_WORKERS` and `DATAFLOW_JOB_QUEUE_CAPACITY`.

### Data processing layer

`CsvDatasetReader` parses CSV data. `Partitioner` divides records into balanced immutable partitions. `DataFlowEngine` performs partition-local counting concurrently and merges the partial maps. Its fixed worker pool also has a bounded task queue. When that queue is full, the submitting job thread waits for capacity (backpressure); submissions fail explicitly if the engine is shutting down.

The CSV reader currently materializes all rows in memory. Large-file streaming and explicit memory budgets are future improvements.

### Error-event intake

`EventController` accepts JSON events at `POST /api/events` and exposes retained groups through `GET /api/issues`. `EventIngestionService` validates bounded field lengths and derives a SHA-256 fingerprint from project, exception type, and a normalized message (numbers and UUIDs are replaced). `IssueStore` synchronizes grouping and idempotency updates so concurrent events cannot lose counts. Repeated event IDs do not increment the group count.

The store retains a bounded number of issue groups and event IDs; when issue capacity is reached, the oldest group is evicted. The Python example client sends one exception synchronously using only the standard library. This is a local learning implementation, not a durable or production-ready telemetry service: groups disappear on restart, there is no authentication or privacy scrubber, and the client does not retry or buffer.

## Reliability behavior

- The application-level job queue is bounded.
- Temporary files are deleted after processing and when job admission is rejected.
- Job lifecycle transitions are guarded against duplicate or invalid transitions.
- The application executor drains accepted work during graceful shutdown, then interrupts remaining tasks after its shutdown timeout. Queued tasks removed during forced shutdown are failed and their temporary files are deleted.
- The partition executor applies backpressure while accepting work and completes futures exceptionally for queued tasks that are discarded during forced shutdown.
- The API does not expose Java stack traces in generic server-error responses.
- Issue aggregation is synchronized and retention-limited; stack traces are not retained.

This is a learning-project baseline, not a production reliability guarantee. Process crashes can lose in-memory jobs; durable recovery, cancellation, retry scheduling, and multi-instance coordination are not implemented.

## Testing strategy

- Unit tests: partition balancing, empty input, invalid partition count, aggregation correctness.
- Concurrency/admission tests: bounded queue saturation and rejection.
- Lifecycle tests: allowed and disallowed job state transitions.
- Event tests: normalized grouping, project isolation, idempotency, bounded retention, and concurrent event counts.
- HTTP integration test: event submission, retry deduplication, and issue lookup through the running Spring application.
- Python client tests: request shape and explicit propagation of network failures.
- Future integration tests: multipart job submission, temporary-file cleanup, persistence, and application shutdown.

## Future architecture work

1. Add integration tests for API submission and polling.
2. Add durable job metadata and results (for example PostgreSQL) behind a repository interface.
3. Add bounded memory/streaming CSV processing and explicit upload limits.
4. Add operational metrics for queue depth, active workers, job duration, failures, and records processed.
5. Add benchmark workloads and publish reproducible measurements.
6. Consider a broker and multi-node workers only if the project requirements genuinely need distributed execution.
