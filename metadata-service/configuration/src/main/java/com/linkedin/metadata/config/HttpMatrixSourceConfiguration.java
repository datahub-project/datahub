package com.linkedin.metadata.config;

import lombok.Data;

/**
 * HTTP-fetched ingestion CLI version matrix configuration. Bound under {@code
 * ingestion.cliVersionMatrix.http} in application.yaml. Each backend defined under {@code
 * cliVersionMatrix} gets its own nested block keyed off the {@code source} discriminator on the
 * parent.
 */
@Data
public class HttpMatrixSourceConfiguration {

  /**
   * URL to a JSON document containing the per-connector version matrix keyed by server release.
   * When empty, no HTTP fetch happens (the factory binds a no-op matrix source).
   */
  private String url;

  /**
   * How often (in seconds) to re-fetch the matrix. Defaults to 600 (10 minutes) via {@code
   * application.yaml}.
   */
  private int refreshSeconds;

  /**
   * Optional value sent verbatim as the {@code Authorization} HTTP header when fetching the matrix.
   * Required when the URL is hosted behind authentication (e.g. a private GitHub gist).
   *
   * <p>Format examples:
   *
   * <ul>
   *   <li>GitHub PAT: {@code "token ghp_xxxxxxxxxxxxxxxx"}
   *   <li>OAuth / OIDC bearer: {@code "Bearer eyJ..."}
   * </ul>
   *
   * <p>The property name ends with "Token" so it is auto-redacted by the system-info properties
   * collector (see {@code PropertiesCollectorConfigurationTest}).
   */
  private String authToken;
}
