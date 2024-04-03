package com.linkedin.datahub.upgrade;

import java.util.List;

/** Responsible for managing the execution of an {@link Upgrade}. */
public interface UpgradeManager {

  /** Register an {@link Upgrade} with the manaager. */
  void register(Upgrade upgrade);

  /** Kick off an {@link Upgrade} by identifier. */
  UpgradeResult execute(String upgradeId, List<String> args);
}
