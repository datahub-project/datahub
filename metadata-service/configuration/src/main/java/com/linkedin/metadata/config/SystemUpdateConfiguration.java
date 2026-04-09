package com.linkedin.metadata.config;

import com.linkedin.metadata.config.kubernetes.KubernetesScaleDownConfiguration;
import lombok.Data;

@Data
@SuppressWarnings("JavadocLinkAsPlainText")
public class SystemUpdateConfiguration {

  private String initialBackOffMs;
  private String maxBackOffs;
  private String backOffFactor;
  private boolean waitForSystemUpdate;
  private boolean cdcMode;

  /** Kubernetes scale-down during system-update (GMS/MAE/MCE) */
  private KubernetesScaleDownConfiguration kubernetesScaleDown;
}
