/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.datahub.upgrade.system;

import com.linkedin.datahub.upgrade.system.bootstrapmcps.BootstrapMCP;
import com.linkedin.datahub.upgrade.system.elasticsearch.steps.DataHubStartupStep;
import java.util.List;
import lombok.NonNull;

public class SystemUpdateBlocking extends SystemUpdate {

  public SystemUpdateBlocking(
      @NonNull List<BlockingSystemUpgrade> blockingSystemUpgrades,
      @NonNull DataHubStartupStep dataHubStartupStep,
      @NonNull final BootstrapMCP bootstrapMCPBlocking) {
    super(blockingSystemUpgrades, List.of(), dataHubStartupStep, bootstrapMCPBlocking, null);
  }
}
