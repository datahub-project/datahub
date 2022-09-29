package com.datahub.plugins.common;

/**
 * Supported plugin types
 */
public enum PluginType {
  AUTHENTICATOR, AUTHORIZER;

  @Override
  public String toString() {
    return this.name().toLowerCase();
  }
}
