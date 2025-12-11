/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.datahub.graphql.exception;

public enum DataHubGraphQLErrorCode {
  BAD_REQUEST(400),
  UNAUTHORIZED(403),
  NOT_FOUND(404),
  CONFLICT(409),
  SERVER_ERROR(500);

  private final int _code;

  public int getCode() {
    return _code;
  }

  DataHubGraphQLErrorCode(final int code) {
    _code = code;
  }
}
