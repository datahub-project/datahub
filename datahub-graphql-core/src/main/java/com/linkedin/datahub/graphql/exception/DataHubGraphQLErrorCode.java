package com.linkedin.datahub.graphql.exception;

public enum DataHubGraphQLErrorCode {
  UNAUTHORIZED(403),
  SERVER_ERROR(500);

  private final int _code;

  public int getCode() {
    return _code;
  }

  DataHubGraphQLErrorCode(final int code) {
    _code = code;
  }
}
