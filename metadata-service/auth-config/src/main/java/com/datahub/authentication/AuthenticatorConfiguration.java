package com.datahub.authentication;

import java.util.Map;
import lombok.Data;

/**
 * POJO representing {@link com.datahub.plugins.auth.authentication.Authenticator} configurations
 * provided in the application.yml.
 */
@Data
public class AuthenticatorConfiguration {
  /**
   * A fully-qualified class name for the {@link
   * com.datahub.plugins.auth.authentication.Authenticator} implementation to be registered.
   */
  private String type;

  /**
   * A set of authenticator-specific configurations passed through during "init" of the
   * authenticator.
   */
  private Map<String, Object> configs;
}
