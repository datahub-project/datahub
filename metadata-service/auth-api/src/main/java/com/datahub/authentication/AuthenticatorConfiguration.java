package com.datahub.authentication;

import java.util.Map;
import lombok.Data;


/**
 * POJO representing {@link Authenticator} configurations provided in the application.yml.
 */
@Data
public class AuthenticatorConfiguration {
  /**
   * A fully-qualified class name for the {@link Authenticator} implementation to be registered.
   */
  private String type;
  /**
   * A set of authenticator-specific configurations passed through during "init" of the authenticator.
   */
  private Map<String, Object> configs;
}
