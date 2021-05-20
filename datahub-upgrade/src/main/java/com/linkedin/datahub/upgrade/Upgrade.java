package com.linkedin.datahub.upgrade;

import java.util.List;

public interface Upgrade {

  /**
   * String identifier for the upgrade operation.
   */
  String id();

  /**
   * Returns a set of steps to perform during the upgrade.
   */
  List<UpgradeStep> steps();

}
