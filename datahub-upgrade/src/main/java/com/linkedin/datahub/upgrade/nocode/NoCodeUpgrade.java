package com.linkedin.datahub.upgrade.nocode;

import com.google.common.collect.ImmutableList;
import com.linkedin.datahub.upgrade.Upgrade;
import com.linkedin.datahub.upgrade.UpgradeCleanupStep;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.models.registry.SnapshotEntityRegistry;
import io.ebean.EbeanServer;
import java.util.ArrayList;
import java.util.List;

public class NoCodeUpgrade implements Upgrade {

  private final List<UpgradeStep<?>> _steps;
  private final List<UpgradeCleanupStep> _cleanupSteps;

  // Upgrade requires the EbeanServer.
  public NoCodeUpgrade(
      final EbeanServer server,
      final EntityService entityService,
      final SnapshotEntityRegistry entityRegistry) {
    _steps = buildUpgradeSteps(
        server,
        entityService,
        entityRegistry);
    _cleanupSteps = buildCleanupSteps(server);
  }

  @Override
  public String id() {
    return "NoCodeDataMigration";
  }

  @Override
  public List<UpgradeStep<?>> steps() {
    return _steps;
  }

  @Override
  public List<UpgradeCleanupStep> cleanupSteps() {
    return _cleanupSteps;
  }

  private List<UpgradeCleanupStep> buildCleanupSteps(final EbeanServer server) {
    return ImmutableList.of(new CleanupStep(server));
  }

  private List<UpgradeStep<?>> buildUpgradeSteps(
      final EbeanServer server,
      final EntityService entityService,
      final SnapshotEntityRegistry entityRegistry) {
    final List<UpgradeStep<?>> steps = new ArrayList<>();
    steps.add(new RemoveAspectV2TableStep(server));
    steps.add(new UpgradeQualificationStep(server));
    steps.add(new CreateAspectTableStep(server));
    steps.add(new IngestDataPlatformsStep(entityService));
    steps.add(new DataMigrationStep(server, entityService, entityRegistry));
    return steps;
  }
}
