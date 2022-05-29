package com.datahub.authorization.ranger;

import java.util.Map;
import java.util.Optional;


public class AuthorizerConfig {
  private final Map<String, Object> authorizerConfigMap;

  public AuthorizerConfig(Map<String, Object> authorizerConfigMap) {
    this.authorizerConfigMap = authorizerConfigMap;
  }

  public String getUsername() {
    return (String) this.authorizerConfigMap.get("username");
  }

  public String getPassword() {
    return (String) this.authorizerConfigMap.get("password");
  }

  public Optional<String> getSslConfig() {
    Optional<String> opt = Optional.ofNullable((String) this.authorizerConfigMap.getOrDefault("sslConfig", null));
    return opt;
  }

  public String getAuthType() {
    return (String) this.authorizerConfigMap.get("authType");
  }
}
