package com.linkedin.datahub.upgrade.kubernetes;

import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Retry count and last-known values for restore, persisted in a ConfigMap. Used only for attempt
 * tracking and for restoring replicas/env on failure—do not use to determine the actual current
 * state of the cluster; current state must be read from the Kubernetes API.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ScaleDownState {

  /** Number of times the scale-down step has run (across job restarts). */
  private int attempt;

  /**
   * Label selectors used for scale-to-zero (from scaleDownDeploymentLabelSelectors). Stored for
   * audit and to know which deployments were targeted.
   */
  private List<String> scaleDownLabelSelectors;

  /**
   * Deployment names resolved from scaleDownLabelSelectors (the deployments we scaled to zero).
   * Stored for audit; restore uses {@link #deployments} for replica counts.
   */
  private List<String> scaleDownDeploymentNames;

  /** Last-known deployment names and replica counts (for restore only). */
  private List<DeploymentReplicas> deployments;

  /**
   * Last-known env values we overwrote per deployment (for restore). Key = deployment name, value =
   * env vars to restore. Covers all deployments that had env updates applied via
   * deploymentEnvUpdates.
   */
  private Map<String, Map<String, String>> envBeforeByDeployment;

  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  public static class DeploymentReplicas {
    private String name;
    private int replicas;
  }
}
