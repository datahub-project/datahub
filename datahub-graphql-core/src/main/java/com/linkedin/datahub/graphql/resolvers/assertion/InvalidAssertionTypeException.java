package com.linkedin.datahub.graphql.resolvers.assertion;

public class InvalidAssertionTypeException extends IllegalArgumentException {

  public InvalidAssertionTypeException(String message) {
    super(message);
  }

  public InvalidAssertionTypeException(String message, Throwable throwable) {
    super(message, throwable);
  }
}
