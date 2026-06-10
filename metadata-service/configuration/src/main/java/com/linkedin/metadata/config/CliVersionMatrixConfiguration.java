package com.linkedin.metadata.config;

import lombok.Data;

/**
 * Per-connector ingestion CLI version matrix configuration. Bound under {@code
 * ingestion.cliVersionMatrix} in application.yaml.
 *
 * <p>Each matrix backend gets its own nested configuration block keyed off {@link #source}, so
 * adding new backends (GMS aspect, AppConfig, etcd, …) does not pile flat properties under {@code
 * ingestion:}.
 */
@Data
public class CliVersionMatrixConfiguration {

  /**
   * Source backend discriminator. Recognised values:
   *
   * <ul>
   *   <li>{@code "http"} (default) — fetch the matrix over HTTP using {@link #http}. When the URL
   *       in that block is empty, the factory wires a no-op source.
   *   <li>{@code "none"} — explicit kill-switch that wins even when an {@code http.url} is set.
   * </ul>
   */
  private String source;

  /** Configuration for the HTTP-fetched matrix backend. */
  private HttpMatrixSourceConfiguration http;
}
