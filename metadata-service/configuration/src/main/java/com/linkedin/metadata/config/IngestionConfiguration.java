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
   * Per-connector CLI version matrix configuration. Nested so additional matrix backends (GMS
   * aspect, AppConfig, …) can be added under their own keys without piling more flat properties on
   * this class. See {@link CliVersionMatrixConfiguration}.
   */
  private CliVersionMatrixConfiguration cliVersionMatrix;

  /**
   * Identifier for this deployment, matched against {@code deployments} entries in the version
   * matrix to select cohort versions. Sourced from {@code DATAHUB_EXECUTOR_CUSTOMER_ID} (injected
   * by the Acryl Cloud Helm chart from the K8s namespace). Empty in single-tenant / OSS
   * deployments, in which case cohort matching never fires and only the per-connector {@code
   * _default} from the matrix applies.
   */
  private String deploymentId;
}
