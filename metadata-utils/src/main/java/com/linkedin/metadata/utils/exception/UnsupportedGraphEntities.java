package com.linkedin.metadata.utils.exception;

/** An exception to be thrown when certain graph entities are not supported. */
public class UnsupportedGraphEntities extends RuntimeException {

  public UnsupportedGraphEntities(String message) {
    super(message);
  }

  public UnsupportedGraphEntities(String message, Throwable throwable) {
    super(message, throwable);
  }
}
