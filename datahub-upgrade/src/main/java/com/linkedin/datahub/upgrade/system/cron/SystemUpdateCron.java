/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.datahub.upgrade.system.cron;

import com.linkedin.datahub.upgrade.Upgrade;
import com.linkedin.datahub.upgrade.UpgradeStep;
import java.util.List;

public class SystemUpdateCron implements Upgrade {

  private List<UpgradeStep> cronSystemUpgrades;

  public SystemUpdateCron(List<UpgradeStep> steps) {
    this.cronSystemUpgrades = steps;
  }

  @Override
  public String id() {
    return this.getClass().getSimpleName();
  }

  @Override
  public List<UpgradeStep> steps() {
    return cronSystemUpgrades;
  }
}
