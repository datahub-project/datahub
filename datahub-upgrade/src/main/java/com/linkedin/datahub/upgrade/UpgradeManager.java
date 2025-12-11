/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.datahub.upgrade;

import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import javax.annotation.Nonnull;

/** Responsible for managing the execution of an {@link Upgrade}. */
public interface UpgradeManager {

  /** Register an {@link Upgrade} with the manaager. */
  UpgradeManager register(Upgrade upgrade);

  /** Kick off an {@link Upgrade} by identifier. */
  UpgradeResult execute(
      @Nonnull OperationContext systemOpContext, String upgradeId, List<String> args);
}
