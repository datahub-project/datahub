package com.linkedin.datahub.graphql.exception;

import graphql.GraphQLException;

/**
 * Exception thrown when authentication fails.
 */
public class AuthorizationException extends GraphQLException {

  public AuthorizationException(String message) {
    super(message);
  }

  public AuthorizationException(String message, Throwable cause) {
    super(message, cause);
  }

}
