# DataFlow 3.0 upgrade notes

This upgrade focuses on separation of processing responsibilities, bounded execution, lifecycle safety, and repeatable quality checks.

## Implemented

- Added `JobExecutionPool`, a dedicated fixed-size executor with a bounded waiting queue.
- Replaced implicit `CompletableFuture.runAsync` submission in `JobService` with explicit executor submission.
- Added overload handling: a full job queue is rejected, the unaccepted in-memory job is removed, and the temporary upload is deleted.
- Added HTTP 503 + `Retry-After: 5` for job queue saturation.
- Added synchronized `QUEUED → RUNNING → COMPLETED|FAILED` transitions to `ProcessingJob`, plus a queued-to-failed transition for work abandoned during forced shutdown.
- Added bounded retention for terminal jobs; queued and running jobs are protected from eviction.
- Added forced-shutdown handling for queued job cleanup and terminal state reporting.
- Added a bounded partition-task queue to `DataFlowEngine`, applying backpressure to submitting job threads.
- Added configurable executor sizes and multipart upload limits.
- Added tests for queue saturation, invalid executor configuration, and job lifecycle transitions.
- Added a GitHub Actions Maven verification workflow.
- Expanded README and architecture documentation.
- Expanded reliability tests for queue saturation, shutdown cleanup, terminal retention, and lifecycle transitions.

## Not implemented in this upgrade

- PostgreSQL persistence or restart recovery.
- Durable retries, cancellation, or dead-letter handling.
- Streaming CSV ingestion (the reader still loads the dataset into memory).
- Authentication/authorization.
- End-to-end HTTP integration tests and load benchmarks.
- Multi-node execution or distributed coordination.

## Verification

The project requires JDK 17+ and Maven 3.9+. Run:

```bash
cd backend
mvn clean verify
```
