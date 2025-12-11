/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.datahub.authentication;

import com.datahub.plugins.auth.authentication.Authenticator;

/**
 * An {@link Exception} thrown when an {@link Authenticator} is unable to be resolve an instance of
 * {@link Authentication} for the current request.
 */
public class AuthenticationExpiredException extends AuthenticationException {

  public AuthenticationExpiredException(final String message) {
    this(message, null);
  }

  public AuthenticationExpiredException(final String message, final Throwable cause) {
    super(message, cause);
  }
}
