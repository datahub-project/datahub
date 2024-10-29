package com.datahub.util.exception;

/** An exception to be thrown when elastic search query fails. */
public class ESQueryException extends RuntimeException {

  public ESQueryException(String message) {
    super(message);
  }

  public ESQueryException(String message, Throwable throwable) {
    super(message, throwable);
  }
}
