package com.dataflow.job;

public class JobQueueFullException extends RuntimeException {
    public JobQueueFullException(String message, Throwable cause) {
        super(message, cause);
    }
}
