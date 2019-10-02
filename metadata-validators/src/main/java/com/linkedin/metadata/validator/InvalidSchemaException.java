package com.linkedin.metadata.validator;

/**
 * Thrown when a schema didn't match the expectation.
 */
public class InvalidSchemaException extends RuntimeException {

  public InvalidSchemaException(String message) {
    super(message);
  }
}
