package com.linkedin.gms.factory.system_telemetry;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * HTTP Basic auth for the {@code /actuator/prometheus} scrape endpoint.
 *
 * <p>When {@link #enabled} is unset ({@code null}), auth is enabled automatically if and only if
 * {@code management.metrics.export.prometheus.enabled} is {@code true}. Set {@code enabled} to
 * {@code false} to expose metrics without auth, or {@code true} to require auth even when export is
 * disabled.
 */
@Data
@ConfigurationProperties(prefix = "management.metrics.export.prometheus.auth")
public class PrometheusScrapeAuthProperties {

  /**
   * When {@code null}, auth follows {@code management.metrics.export.prometheus.enabled}. When
   * {@code true} or {@code false}, overrides that default.
   */
  private Boolean enabled;

  /** Scrape username; defaults to {@value PrometheusScrapeAuthSettings#DEFAULT_USERNAME}. */
  private String username;

  /** Scrape password; defaults to {@value PrometheusScrapeAuthSettings#DEFAULT_PASSWORD}. */
  private String password;
}
