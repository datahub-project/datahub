package com.linkedin.metadata.config;

import lombok.Data;

/**
 * Per-connector ingestion CLI version matrix configuration. Bound under {@code
 * ingestion.cliVersionMatrix} in application.yaml.
 *
 * <p>The nested structure replaces an earlier set of flat {@code versionMatrix*} keys on the parent
 * {@link IngestionConfiguration}. Keeping all matrix configuration under one block makes adding
 * future backends (GMS aspect, AppConfig, etcd, …) a localized change — each new backend gets its
 * own nested block keyed off {@link #source}.
 */
@Data
public class CliVersionMatrixConfiguration {

  /**
   * Source backend discriminator. Recognised values:
   *
   * <ul>
   *   <li>{@code "http"} — fetch via HTTP using {@link #http}
   *   <li>{@code "none"} — explicit disable (no matrix consulted)
   *   <li>unset / empty — backward-compatible auto-inference: if {@code http.url} is set, treat as
   *       {@code "http"}; otherwise treat as {@code "none"}. Lets existing deployments that
   *       pre-date this discriminator continue working without an env change.
   * </ul>
   */
  private String source;

  /**
   * Configuration for the HTTP-fetched matrix backend. Always present so the factory can read
   * {@code http.url} for the backward-compat auto-inference path even when {@code source} is unset.
   */
  private HttpMatrixSourceConfiguration http;
}
