package com.linkedin.metadata.models;

public class ModelValidationException extends RuntimeException {

  public ModelValidationException(String message) {
    super(message);
  }

  public ModelValidationException(String message, Throwable cause) {
    super(message, cause);
  }

  public ModelValidationException(Throwable cause) {
    super(cause);
  }
}
