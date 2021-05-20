package com.linkedin.datahub.upgrade.nocode;

import com.linkedin.datahub.upgrade.Upgrade;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.models.registry.SnapshotEntityRegistry;
import io.ebean.EbeanServer;
import java.util.ArrayList;
import java.util.List;


/**
 * Note that we will not "qualify" the upgrade.
 */
public class NoCodeUpgrade implements Upgrade {

  private final List<UpgradeStep> _steps;

  public NoCodeUpgrade(
      final EbeanServer server,
      final EntityService entityService,
      final SnapshotEntityRegistry entityRegistry
  ) {
    this(server, entityService, entityRegistry, true);
  }

  // Upgrade requires the EbeanServer.
  public NoCodeUpgrade(
      final EbeanServer server,
      final EntityService entityService,
      final SnapshotEntityRegistry entityRegistry,
      final boolean shouldRunQualification) {
    _steps = buildUpgradeSteps(
        server,
        entityService,
        entityRegistry,
        shouldRunQualification);
  }

  @Override
  public String id() {
    return "NoCodeDataMigration";
  }

  @Override
  public List<UpgradeStep> steps() {
    return _steps;
  }

  private List<UpgradeStep> buildUpgradeSteps(
      final EbeanServer server,
      final EntityService entityService,
      final SnapshotEntityRegistry entityRegistry,
      final boolean shouldRunQualification) {
    final List<UpgradeStep> steps = new ArrayList<>();
    if (shouldRunQualification) {
      steps.add(new UpgradeQualificationStep(server));
    }
    steps.add(new CreateAspectTableStep(server));
    steps.add(new IngestDataPlatformsStep(entityService));
    steps.add(new DataMigrationStep(server, entityService, entityRegistry));
    return steps;
  }
}
