package com.linkedin.gms.factory.system_telemetry;

import lombok.Value;

/** Resolved HTTP Basic auth settings for {@code /actuator/prometheus}. */
@Value
public class PrometheusScrapeAuthSettings {

  public static final String DEFAULT_USERNAME = "prometheus";
  public static final String DEFAULT_PASSWORD = "datahub";

  boolean enabled;
  String username;
  String password;
  boolean usingDefaultPassword;

  public static PrometheusScrapeAuthSettings resolve(
      boolean prometheusExportEnabled,
      Boolean authEnabledOverride,
      String username,
      String password) {
    final boolean enabled =
        authEnabledOverride != null ? authEnabledOverride : prometheusExportEnabled;
    final String effectiveUsername =
        username != null && !username.isBlank() ? username : DEFAULT_USERNAME;
    final String effectivePassword =
        password != null && !password.isBlank() ? password : DEFAULT_PASSWORD;
    final boolean usingDefaultPassword = enabled && (password == null || password.isBlank());
    return new PrometheusScrapeAuthSettings(
        enabled, effectiveUsername, effectivePassword, usingDefaultPassword);
  }
}
