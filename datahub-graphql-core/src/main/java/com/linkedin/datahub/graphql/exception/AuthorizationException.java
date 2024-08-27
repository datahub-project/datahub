package com.linkedin.datahub.graphql.exception;

/** Exception thrown when authentication fails. */
public class AuthorizationException extends DataHubGraphQLException {

  public AuthorizationException(String message) {
    super(message, DataHubGraphQLErrorCode.UNAUTHORIZED);
  }

  public AuthorizationException(String message, Throwable cause) {
    super(message, DataHubGraphQLErrorCode.UNAUTHORIZED);
  }
}
