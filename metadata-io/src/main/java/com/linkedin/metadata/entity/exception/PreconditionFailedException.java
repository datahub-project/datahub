package com.linkedin.metadata.entity.exception;

public class PreconditionFailedException extends RuntimeException {
    public PreconditionFailedException(String message) {
        super(message);
    }
}
