package com.linkedin.metadata.config;

import lombok.Data;

/**
 * Per-connector ingestion CLI version matrix configuration. Bound under {@code
 * ingestion.cliVersionMatrix} in application.yaml.
 */
@Data
public class CliVersionMatrixConfiguration {

  /**
   * Location of the matrix JSON document. The URI scheme selects the backend, so supporting another
   * store never adds another set of properties:
   *
   * <ul>
   *   <li>{@code s3://bucket/key.json} — read with GMS's ambient AWS credentials. Requests are
   *       SigV4-signed, so the bucket can stay private and be shared cross-account by bucket
   *       policy.
   *   <li>{@code gs://bucket/key.json} — read with Application Default Credentials (Workload
   *       Identity Federation on GKE), so no static key file is needed.
   *   <li>{@code file:///absolute/path.json} — read from the local filesystem.
   *   <li>{@code http://host/path} or {@code https://host/path} — fetched over HTTP, optionally
   *       authenticated with {@link #authToken}.
   * </ul>
   *
   * <p>Empty (the default) disables matrix lookups and connectors use the application default CLI
   * version. That is also the failure mode for an unsupported scheme, a URI missing its object key,
   * or an unavailable storage client: the factory logs and wires a no-op source rather than
   * blocking GMS startup.
   */
  private String uri;

  /**
   * Optional value sent verbatim as the {@code Authorization} header. Applies to {@code http(s)}
   * URIs only — {@code s3} and {@code gs} authenticate with ambient cloud credentials, and {@code
   * file} needs nothing. Property name ends with "Token" so it is auto-redacted in system-info.
   */
  private String authToken;

  /**
   * How often (in seconds) to re-read the matrix. The 600s (10 minute) default is supplied by
   * application.yaml; the field itself has no default (a bare {@code int} is 0), and the factory
   * treats a non-positive value as a misconfiguration and degrades to a no-op source.
   */
  private int refreshSeconds;
}
