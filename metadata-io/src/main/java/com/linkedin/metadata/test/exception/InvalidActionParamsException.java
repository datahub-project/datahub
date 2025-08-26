package com.linkedin.metadata.test.exception;

/**
 * An exception to be thrown when action applier does not have required parameters to apply action
 */
public class InvalidActionParamsException extends RuntimeException {

  public InvalidActionParamsException(String message) {
    super(message);
  }

  public InvalidActionParamsException(String message, Throwable throwable) {
    super(message, throwable);
  }
}
