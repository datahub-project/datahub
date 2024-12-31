package com.datahub.util.exception;

/** Thrown when a schema didn't match the expectation. */
public class InvalidSchemaException extends RuntimeException {

  public InvalidSchemaException(String message) {
    super(message);
  }
}
