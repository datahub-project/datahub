package com.datahub.plugins.auth.configuration;

import java.util.Map;
import java.util.Optional;
import lombok.Data;


/**
 * POJO for YAML section presents in config.yml at location plugins[].params.
 *
 * These parameters are same for Authenticator and Authorizer plugins.
 *
 * {@link com.datahub.plugins.auth.provider.AuthPluginConfigProvider} uses this AuthParam to create instance of
 * either {@link AuthenticatorPluginConfig} or {@link AuthorizerPluginConfig}
 */
@Data
public class AuthParam {
  /**
   * Fully-qualified class-name of plugin
   */
  private String className;

  /**
   * Default jarFileName is "<plugin-name>.jar". If plugin's jar file name is different from default value then set
   * this property.
   */
  private Optional<String> jarFileName = Optional.empty();

  /**
   * These configs are specific to plugin. GMS pass this map as is to plugin
   * {@link com.datahub.plugins.auth.authentication.Authenticator} or
   * {@link com.datahub.plugins.auth.authorization.Authorizer} init method
   */
  private Optional<Map<String, Object>> configs = Optional.empty();
}
