package com.linkedin.datahub.upgrade.system;

import com.linkedin.datahub.upgrade.system.elasticsearch.steps.DataHubStartupStep;
import java.util.List;
import lombok.NonNull;
import org.jetbrains.annotations.Nullable;

public class SystemUpdateBlocking extends SystemUpdate {

  public SystemUpdateBlocking(
      @NonNull List<BlockingSystemUpgrade> blockingSystemUpgrades,
      @NonNull List<NonBlockingSystemUpgrade> nonBlockingSystemUpgrades,
      @Nullable DataHubStartupStep dataHubStartupStep) {
    super(blockingSystemUpgrades, nonBlockingSystemUpgrades, dataHubStartupStep);
  }
}
