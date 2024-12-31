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
