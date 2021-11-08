package com.datahub.authentication.token;

public class DataHubTokenException extends RuntimeException {

  public DataHubTokenException(final String message) {
    super(message);
  }

  public DataHubTokenException(final String message, final Throwable e) {
    super(message, e);
  }
}
