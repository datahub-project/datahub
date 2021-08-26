package com.linkedin.datahub.graphql.exception;


/**
 * Exception thrown when authentication fails.
 */
public class AuthorizationException extends CustomGraphQLException {

  public AuthorizationException(String message) {
    super(message, CustomGraphQLErrorCode.UNAUTHORIZED);
  }

  public AuthorizationException(String message, Throwable cause) {
    super(message, CustomGraphQLErrorCode.UNAUTHORIZED);
  }
}
