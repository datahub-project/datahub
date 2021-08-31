package com.linkedin.datahub.graphql.exception;

public enum CustomGraphQLErrorCode {
  UNAUTHORIZED(403),
  SERVER_ERROR(500);

  private final int _code;

  public int getCode() {
    return _code;
  }

  CustomGraphQLErrorCode(final int code) {
    _code = code;
  }
}
