package com.linkedin.datahub.upgrade.system;

import com.linkedin.datahub.upgrade.Upgrade;
import com.linkedin.datahub.upgrade.UpgradeCleanupStep;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.system.elasticsearch.steps.DataHubStartupStep;
import java.util.LinkedList;
import java.util.List;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;

@Getter
@Slf4j
@Accessors(fluent = true)
public class SystemUpdate implements Upgrade {
  private final List<UpgradeStep> steps;
  private final List<UpgradeCleanupStep> cleanupSteps;

  public SystemUpdate(
      @NonNull final List<BlockingSystemUpgrade> blockingSystemUpgrades,
      @NonNull final List<NonBlockingSystemUpgrade> nonBlockingSystemUpgrades,
      @Nullable final DataHubStartupStep dataHubStartupStep) {

    steps = new LinkedList<>();
    cleanupSteps = new LinkedList<>();

    // blocking upgrades
    steps.addAll(blockingSystemUpgrades.stream().flatMap(up -> up.steps().stream()).toList());
    cleanupSteps.addAll(
        blockingSystemUpgrades.stream().flatMap(up -> up.cleanupSteps().stream()).toList());

    // emit system update message if blocking upgrade(s) present
    if (dataHubStartupStep != null && !blockingSystemUpgrades.isEmpty()) {
      steps.add(dataHubStartupStep);
    }

    // add non-blocking upgrades last
    steps.addAll(nonBlockingSystemUpgrades.stream().flatMap(up -> up.steps().stream()).toList());
    cleanupSteps.addAll(
        nonBlockingSystemUpgrades.stream().flatMap(up -> up.cleanupSteps().stream()).toList());
  }

  @Override
  public String id() {
    return getClass().getSimpleName();
  }
}
