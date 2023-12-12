package com.linkedin.datahub.graphql.exception;

import graphql.GraphQLException;

public class DataHubGraphQLException extends GraphQLException {

  private final DataHubGraphQLErrorCode code;

  public DataHubGraphQLException(String message, DataHubGraphQLErrorCode code) {
    super(message);
    this.code = code;
  }

  public DataHubGraphQLException(String message, DataHubGraphQLErrorCode code, Throwable cause) {
    super(message, cause);
    this.code = code;
  }

  public DataHubGraphQLErrorCode errorCode() {
    return this.code;
  }
}
