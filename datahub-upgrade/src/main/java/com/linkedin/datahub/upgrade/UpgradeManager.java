package com.linkedin.datahub.upgrade;


/**
 * Responsible for managing the execution of an {@link Upgrade}.
 */
public interface UpgradeManager {

  /**
   * Register an {@link Upgrade} with the manaager.
   */
  void register(Upgrade upgrade);

  /**
   * Kick off an {@link Upgrade} by identifier.
   */
  UpgradeResult execute(String upgradeId, String... args);

}
