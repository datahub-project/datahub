package com.datahub.authorization.ranger;

import lombok.Builder;
import lombok.Getter;


@Getter
@Builder
public class AuthorizerConfig {
  public static final String CONFIG_USERNAME = "username";
  public static final String CONFIG_PASSWORD = "password";

  private final String username;
  private final String password;

  public static CustomBuilder builder() {
    return new CustomBuilder();
  }

  public static class CustomBuilder extends AuthorizerConfigBuilder {

    public AuthorizerConfig build() {

      if (super.username == null || super.username.trim().length() == 0) {
        throw new IllegalArgumentException("username should empty");
      }

      if (super.password == null || super.password.trim().length() == 0) {
        throw new IllegalArgumentException("password should empty");
      }

      return super.build();
    }
  }
}
