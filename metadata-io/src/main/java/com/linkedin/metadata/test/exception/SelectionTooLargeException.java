package com.linkedin.metadata.test.exception;

import lombok.Getter;

public class SelectionTooLargeException extends RuntimeException {

  public SelectionTooLargeException(String message, int count) {
    super(message);
    this.count = count;
  }

  @Getter private int count;

  public SelectionTooLargeException(String message, Throwable throwable) {
    super(message, throwable);
  }
}
