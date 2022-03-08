package com.datahub.authentication.token;

/**
 * Represents a type of JWT access token granted by the {@link TokenService}.
 */
public enum TokenType {
  /**
   * A UI-initiated session token
   */
  SESSION,
  /**
   * A personal token for programmatic use
   */
  PERSONAL,
}
