package com.linkedin.datahub.graphql.exception;

import graphql.GraphQLException;


public class CustomGraphQLException extends GraphQLException {

  private final CustomGraphQLErrorCode code;

  public CustomGraphQLException(String message, CustomGraphQLErrorCode code) {
    super(message);
    this.code = code;
  }

  public CustomGraphQLException(String message, CustomGraphQLErrorCode code, Throwable cause) {
    super(message, cause);
    this.code = code;
  }

  public CustomGraphQLErrorCode errorCode() {
    return this.code;
  }
}
