/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

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
