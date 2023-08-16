package com.linkedin.datahub.graphql.exception;

public enum DataHubGraphQLErrorCode {
  BAD_REQUEST(400),
  UNAUTHORIZED(403),
  NOT_FOUND(404),
  UNAUTHORIZED_TAG_ERROR(405),
  SERVER_ERROR(500);

  private final int _code;

  public int getCode() {
    return _code;
  }

  DataHubGraphQLErrorCode(final int code) {
    _code = code;
  }
}
