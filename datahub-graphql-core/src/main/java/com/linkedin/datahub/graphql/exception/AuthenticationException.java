package com.linkedin.datahub.graphql.exception;

import graphql.GraphQLException;

/** Exception thrown when authentication fails. */
public class AuthenticationException extends GraphQLException {

  public AuthenticationException(String message) {
    super(message);
  }

  public AuthenticationException(String message, Throwable cause) {
    super(message, cause);
  }
}
