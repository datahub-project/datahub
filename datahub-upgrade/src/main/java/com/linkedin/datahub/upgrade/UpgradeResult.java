package com.linkedin.datahub.upgrade;

import com.linkedin.upgrade.DataHubUpgradeState;

/** Represents the result of executing an {@link Upgrade} */
public interface UpgradeResult {

  /** Returns the {@link DataHubUpgradeState} of executing an {@link Upgrade} */
  DataHubUpgradeState result();

  /** Returns the {@link UpgradeReport} associated with the completed {@link Upgrade}. */
  UpgradeReport report();
}
