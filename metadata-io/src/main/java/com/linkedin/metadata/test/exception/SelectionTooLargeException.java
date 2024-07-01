package com.linkedin.metadata.test.exception;

public class SelectionTooLargeException extends RuntimeException {

  public SelectionTooLargeException(String message) {
    super(message);
  }

  public SelectionTooLargeException(String message, Throwable throwable) {
    super(message, throwable);
  }
}
