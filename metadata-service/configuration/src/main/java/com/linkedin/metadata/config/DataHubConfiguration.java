/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

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

  @Data
  public static class DataHubMetrics {
    private MetricsOptions hookLatency;
  }
}
