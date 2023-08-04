package io.datahub.test.exception;

/**
 * An exception to be thrown when operator does not have required operands to apply operator
 */
public class InvalidOperandException extends RuntimeException {

  public InvalidOperandException(String message) {
    super(message);
  }

  public InvalidOperandException(String message, Throwable throwable) {
    super(message, throwable);
  }
}
