package com.linkedin.metadata.entity.datastax;

public class ConditionalWriteFailedException extends RuntimeException {

    public ConditionalWriteFailedException(String message) {
        super(message);
    }

    public ConditionalWriteFailedException(String message, Throwable throwable) {
        super(message, throwable);
    }
}
