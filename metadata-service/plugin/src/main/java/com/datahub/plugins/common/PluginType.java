package com.datahub.plugins.common;

/**
 * Supported plugin types
 */
public enum PluginType {
  /**
   * PluginType for Authenticator plugin
   */
  AUTHENTICATOR,

  /**
   * PluginType for Authorizer plugin
   */
  AUTHORIZER;

  @Override
  public String toString() {
    return this.name().toLowerCase();
  }
}
