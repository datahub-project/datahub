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
