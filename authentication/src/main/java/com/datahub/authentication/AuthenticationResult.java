package com.datahub.authentication;

/**
 * Stores the result returned by a single {@link Authenticator}.
 */
public class AuthenticationResult {
  public enum Type {
    SUCCESS,
    FAILURE
  }
  private final Type type;
  private Authentication authentication;

  public AuthenticationResult(
      Type type,
      Authentication authentication) {
    this.type = type;
    this.authentication = authentication;
  }

  public Type type() {
    return this.type;
  }

  public Authentication authentication() {
    return this.authentication;
  }
}
