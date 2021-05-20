package com.linkedin.datahub.upgrade;

import java.util.List;


public interface UpgradeContext {

  /**
   * Get the currently running upgrade.
   */
  Upgrade upgrade();

  List<UpgradeStepResult> stepResults();

  UpgradeReport report();

  List<String> args();

}
