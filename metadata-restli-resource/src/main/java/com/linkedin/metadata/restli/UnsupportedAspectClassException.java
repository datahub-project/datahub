package com.linkedin.metadata.restli;

public class UnsupportedAspectClassException extends RuntimeException {

  public UnsupportedAspectClassException(String message) {
    super(message);
  }

  public UnsupportedAspectClassException(String message, Throwable throwable) {
    super(message, throwable);
  }
}
