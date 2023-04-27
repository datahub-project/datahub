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
