package com.datahub.authorization;

import java.util.Map;
import lombok.Data;


/**
 * POJO representing {@link Authorizer} configurations provided in the application.yml.
 */
@Data
public class AuthorizerConfiguration {
  /**
   * A fully-qualified class name for the {@link Authorizer} implementation to be registered.
   */
  private String type;
  /**
   * A set of authorizer-specific configurations passed through during "init" of the authorizer.
   */
  private Map<String, Object> configs;
}
