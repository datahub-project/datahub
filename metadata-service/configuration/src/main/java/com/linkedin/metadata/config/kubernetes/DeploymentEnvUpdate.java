package com.linkedin.metadata.config.kubernetes;

import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * One entry in deploymentEnvUpdates config: a label selector that resolves to a deployment and the
 * env vars to set on it when scaling down. Used for JSON parsing of
 * systemUpdate.kubernetesScaleDown configuration.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class DeploymentEnvUpdate {

  private String labelSelector;
  private Map<String, String> env;
}
