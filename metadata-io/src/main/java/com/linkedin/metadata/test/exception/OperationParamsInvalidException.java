package com.linkedin.metadata.test.exception;

/**
 * An exception to be thrown when operator does not have required params to apply operator
 */
public class OperationParamsInvalidException extends RuntimeException {

  public OperationParamsInvalidException(String message) {
    super(message);
  }

  public OperationParamsInvalidException(String message, Throwable throwable) {
    super(message, throwable);
  }
}
