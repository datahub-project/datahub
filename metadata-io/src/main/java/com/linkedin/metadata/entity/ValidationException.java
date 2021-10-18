package com.linkedin.metadata.entity;

/**
 * Exception thrown when a metadata record cannot be validated against its schema.
 */
public class ValidationException extends RuntimeException {
  public ValidationException(final String message) {
    super(message);
  }
}
