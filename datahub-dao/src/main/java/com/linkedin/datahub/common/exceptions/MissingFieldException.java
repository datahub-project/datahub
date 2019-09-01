package com.linkedin.datahub.common.exceptions;

/**
 * An exception to be thrown when certain required field is missing.
 */
public class MissingFieldException extends RuntimeException {

  public MissingFieldException(String message) {
    super(message);
  }
}
