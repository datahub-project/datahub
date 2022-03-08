package com.datahub.util.exception;

public class RetryLimitReached extends RuntimeException {

  public RetryLimitReached(String message) {
    super(message);
  }

  public RetryLimitReached(String message, Throwable throwable) {
    super(message, throwable);
  }
}
