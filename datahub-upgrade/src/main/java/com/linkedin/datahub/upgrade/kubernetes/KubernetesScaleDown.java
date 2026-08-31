package com.linkedin.datahub.upgrade.kubernetes;

import com.google.common.collect.ImmutableList;
import com.linkedin.datahub.upgrade.UpgradeCleanupStep;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.system.BlockingSystemUpgrade;
import java.util.List;

/** Blocking system upgrade that scales down GMS/MAE/MCE in Kubernetes before other steps run. */
public class KubernetesScaleDown implements BlockingSystemUpgrade {

  private final List<UpgradeStep> steps;
  private final List<UpgradeCleanupStep> cleanupSteps;

  public KubernetesScaleDown(
      KubernetesScaleDownStep scaleDownStep, KubernetesScaleDownCleanupStep cleanupStep) {
    this.steps = ImmutableList.of(scaleDownStep);
    this.cleanupSteps = ImmutableList.of(cleanupStep);
  }

  @Override
  public String id() {
    return "KubernetesScaleDown";
  }

  @Override
  public boolean requiresK8ScaleDown(UpgradeContext context) {
    return false;
  }

  @Override
  public List<UpgradeStep> steps() {
    return steps;
  }

  @Override
  public List<UpgradeCleanupStep> cleanupSteps() {
    return cleanupSteps;
  }
}
