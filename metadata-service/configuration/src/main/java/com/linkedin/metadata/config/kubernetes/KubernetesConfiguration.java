package com.linkedin.metadata.config.kubernetes;

import lombok.Data;

/**
 * Global Kubernetes environment configuration. When the process runs inside a Kubernetes cluster,
 * the cluster sets KUBERNETES_SERVICE_HOST; this is bound in application.yaml and can be used by
 * any component that needs to detect "running in K8" (e.g. system-update scale-down, OpenAPI K8
 * operations).
 */
@Data
public class KubernetesConfiguration {

  /**
   * When non-empty, indicates the process is running inside a Kubernetes cluster. Bound from
   * KUBERNETES_SERVICE_HOST in application.yaml (set by the cluster when the pod runs). Empty when
   * not in K8.
   */
  private String serviceHost;
}
