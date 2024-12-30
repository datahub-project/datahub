package io.datahubproject.metadata.exception;

public class OperationContextException extends RuntimeException {
  public OperationContextException(String message) {
    super(message);
  }

  public OperationContextException() {}
}
