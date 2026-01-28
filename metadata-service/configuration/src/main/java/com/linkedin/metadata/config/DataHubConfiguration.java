package com.linkedin.metadata.config;

import lombok.Data;

/** POJO representing the "datahub" configuration block in application.yaml. */
@Data
public class DataHubConfiguration {
  /**
   * Indicates the type of server that has been deployed: quickstart, prod, or a custom
   * configuration
   */
  public String serverType;

  public String serverEnv;

  /** The base path for the URL where DataHub will be deployed */
  private String basePath;

  /**
   * Deployment mode where DataHub will drop writes to the system blindly. Intended as a special
   * deployment mode for pointing to read replicas as an offline analytics tool.
   */
  private boolean readOnly = false;

  /** GMS (Graph Metadata Service) configuration */
  private GMSConfiguration gms;

  private PluginConfiguration plugin;

  private DataHubMetrics metrics;

  private PoliciesConfiguration policies;

  private S3Configuration s3;

  private ValidationConfiguration validation;

  @Data
  public static class DataHubMetrics {
    private MetricsOptions hookLatency;
  }
}
