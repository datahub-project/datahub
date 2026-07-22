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
   *   <li>{@code "s3"} — read the matrix from an S3 object using {@link #s3}. When the bucket is
   *       empty or no S3 client is available, the factory wires a no-op source.
   *   <li>{@code "gcs"} — read the matrix from a GCS object using {@link #gcs}. When the bucket is
   *       empty or no GCS client is available, the factory wires a no-op source.
   *   <li>{@code "none"} — explicit kill-switch that wins even when an {@code http.url} is set.
   * </ul>
   */
  private String source;

  /** Configuration for the HTTP-fetched matrix backend. */
  private HttpMatrixSourceConfiguration http;

  /** Configuration for the S3-fetched matrix backend. */
  private S3MatrixSourceConfiguration s3;

  /** Configuration for the GCS-fetched matrix backend. */
  private GcsMatrixSourceConfiguration gcs;
}
