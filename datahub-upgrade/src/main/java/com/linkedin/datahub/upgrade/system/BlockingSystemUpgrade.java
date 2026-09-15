package com.linkedin.datahub.upgrade.system;

import com.linkedin.datahub.upgrade.Upgrade;
import com.linkedin.datahub.upgrade.UpgradeContext;

/** Blocking upgrade that can vote on whether K8 scale-down is required before steps run. */
public interface BlockingSystemUpgrade extends Upgrade {

  /**
   * Returns true if this upgrade needs K8 scale-down (e.g. BuildIndices when reindex is needed).
   * Called during ScaleDownEvaluationStep; default false.
   */
  default boolean requiresK8ScaleDown(UpgradeContext context) {
    return false;
  }
}
