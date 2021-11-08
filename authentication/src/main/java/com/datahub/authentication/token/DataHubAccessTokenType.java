package com.datahub.authentication.token;

public enum DataHubAccessTokenType {
  /**
   * A UI-initiated session token
   */
  SESSION,
  /**
   * A personal token for programmatic use
   */
  PERSONAL,
  /**
   * A service principal token
   */
  SERVICE
}
