package com.linkedin.datahub.upgrade.system;

import com.linkedin.datahub.upgrade.Upgrade;
import com.linkedin.datahub.upgrade.UpgradeCleanupStep;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.system.elasticsearch.BuildIndices;
import com.linkedin.datahub.upgrade.system.elasticsearch.CleanIndices;
import com.linkedin.datahub.upgrade.system.elasticsearch.steps.DataHubStartupStep;
import com.linkedin.datahub.upgrade.system.entity.steps.BackfillBrowsePathsV2;
import com.linkedin.metadata.dao.producer.KafkaEventProducer;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SystemUpdate implements Upgrade {
  private final List<Upgrade> _preStartupUpgrades;
  private final List<Upgrade> _postStartupUpgrades;
  private final List<UpgradeStep> _steps;

  public SystemUpdate(
      final BuildIndices buildIndicesJob,
      final CleanIndices cleanIndicesJob,
      final KafkaEventProducer kafkaEventProducer,
      final String version,
      final BackfillBrowsePathsV2 backfillBrowsePathsV2) {

    _preStartupUpgrades = List.of(buildIndicesJob);
    _steps = List.of(new DataHubStartupStep(kafkaEventProducer, version));
    _postStartupUpgrades = List.of(cleanIndicesJob, backfillBrowsePathsV2);
  }

  @Override
  public String id() {
    return "SystemUpdate";
  }

  @Override
  public List<UpgradeStep> steps() {
    return Stream.concat(
            Stream.concat(
                _preStartupUpgrades.stream().flatMap(up -> up.steps().stream()), _steps.stream()),
            _postStartupUpgrades.stream().flatMap(up -> up.steps().stream()))
        .collect(Collectors.toList());
  }

  @Override
  public List<UpgradeCleanupStep> cleanupSteps() {
    return Stream.concat(
            _preStartupUpgrades.stream().flatMap(up -> up.cleanupSteps().stream()),
            _postStartupUpgrades.stream().flatMap(up -> up.cleanupSteps().stream()))
        .collect(Collectors.toList());
  }
}
