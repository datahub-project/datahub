package com.datahub.authentication;

/**
 * Thrown by an {@link com.datahub.plugins.auth.authentication.Authenticator} when it could not
 * decide whether the credentials are valid because a backing store (for example the database that
 * holds access-token records) failed. The credentials may be valid, so callers should answer with a
 * retryable error (HTTP 503), not with 401.
 */
public class AuthenticationUnavailableException extends AuthenticationException {

  public AuthenticationUnavailableException(final String message, final Throwable cause) {
    super(message, cause);
  }
}
