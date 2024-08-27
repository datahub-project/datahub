package com.linkedin.datahub.graphql.exception;

import graphql.GraphQLException;

/** Exception thrown when an unexpected value is provided by the client. */
public class ValidationException extends GraphQLException {

  public ValidationException(String message) {
    super(message);
  }

  public ValidationException(String message, Throwable cause) {
    super(message, cause);
  }
}
