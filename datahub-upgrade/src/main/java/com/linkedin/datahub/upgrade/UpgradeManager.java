package com.linkedin.datahub.upgrade;


/**
 * Responsible for executing an {@link Upgrade}
 */
public interface UpgradeManager {

  void register(Upgrade upgrade);

  UpgradeResult execute(String upgradeId, String... args);

}
