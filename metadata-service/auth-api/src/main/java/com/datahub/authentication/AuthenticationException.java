package com.datahub.authentication;

/**
 * An {@link Exception} thrown when an {@link Authenticator} is unable to be resolve an instance of
 * {@link Authentication} for the current request.
 */
public class AuthenticationException extends Exception {

  public AuthenticationException(final String message) {
    this(message, null);
  }

  public AuthenticationException(final String message, final Throwable cause) {
    super(String.format("Failed to authenticate inbound request: %s", message), cause);
  }
}
