package com.linkedin.datahub.upgrade;

import com.google.common.collect.ImmutableList;
import java.util.List;

/** Specification of an upgrade to be performed to the DataHub platform. */
public interface Upgrade {

  /** String identifier for the upgrade. */
  String id();

  /** Returns a set of steps to perform during the upgrade. */
  List<UpgradeStep> steps();

  /** Returns a set of steps to perform on upgrade success, failure, or abort. */
  default List<UpgradeCleanupStep> cleanupSteps() {
    return ImmutableList.of();
  }
}
