package com.linkedin.metadata.filter.auth;

public class AuthenticationResultImpl implements AuthenticationResult {

  private final Principal principal;

  public AuthenticationResultImpl(final Principal principal) {
    this.principal = principal;
  }
  @Override
  public Principal principal() {
    return this.principal;
  }
}
