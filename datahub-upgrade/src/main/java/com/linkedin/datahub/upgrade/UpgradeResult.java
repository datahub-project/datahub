/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.datahub.upgrade;

import com.linkedin.upgrade.DataHubUpgradeState;

/** Represents the result of executing an {@link Upgrade} */
public interface UpgradeResult {

  /** Returns the {@link DataHubUpgradeState} of executing an {@link Upgrade} */
  DataHubUpgradeState result();

  /** Returns the {@link UpgradeReport} associated with the completed {@link Upgrade}. */
  UpgradeReport report();
}
