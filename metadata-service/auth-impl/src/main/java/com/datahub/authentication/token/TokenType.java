package com.datahub.authentication.token;

/**
 * Represents a type of JWT access token granted by the {@link StatelessTokenService}.
 */
public enum TokenType {

  /**
   * A UI-initiated session token
   */
  SESSION(true),
  /**
   * A personal token for programmatic use
   */
  PERSONAL(false);

  private final boolean expirationMandatory;

  private TokenType(boolean expirationMandatory) {
    this.expirationMandatory = expirationMandatory;
  }

  public boolean isExpirationMandatory() {
    return expirationMandatory;
  }
}
