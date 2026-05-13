package com.linkedin.metadata.config;

import lombok.Data;

/** POJO representing the "ingestion" configuration block in application.yaml. */
@Data
public class IngestionConfiguration {
  /** Whether managed ingestion is enabled */
  private boolean enabled;

  /** The default CLI version to use in managed ingestion */
  private String defaultCliVersion;

  private Integer batchRefreshCount;

  /**
   * Optional URL to a publicly accessible JSON file containing a per-connector version matrix keyed
   * by server release version. When set, the server fetches and caches this matrix and uses it to
   * resolve the CLI version per connector type. When empty, the existing defaultCliVersion is used
   * for all connectors.
   */
  private String versionMatrixUrl;

  /**
   * How often (in seconds) to re-fetch the version matrix from versionMatrixUrl. Defaults to 600
   * (10 minutes).
   */
  private int versionMatrixRefreshSeconds;

  /**
   * Optional value sent verbatim as the {@code Authorization} HTTP header when fetching the version
   * matrix. Required when the matrix URL is hosted behind authentication (e.g. a private GitHub
   * repo's {@code raw.githubusercontent.com} URL).
   *
   * <p>Format is whatever the host expects:
   *
   * <ul>
   *   <li>GitHub PAT: {@code "token ghp_xxxxxxxxxxxxxxxx"}
   *   <li>OAuth / OIDC bearer: {@code "Bearer eyJ..."}
   * </ul>
   *
   * <p>When empty or unset, no {@code Authorization} header is sent (public-URL semantics).
   */
  private String versionMatrixAuthToken;

  /**
   * Identifier for this deployment, matched against {@code deployments} entries in the version
   * matrix to select cohort versions. Sourced from {@code DATAHUB_EXECUTOR_CUSTOMER_ID} (injected
   * by the Acryl Cloud Helm chart from the K8s namespace). Empty in single-tenant / OSS
   * deployments, in which case cohort matching never fires and only the per-connector {@code
   * _default} from the matrix applies.
   */
  private String deploymentId;
}
