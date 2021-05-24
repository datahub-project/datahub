package com.linkedin.datahub.upgrade.nocodecleanup;

import com.linkedin.datahub.upgrade.Upgrade;
import com.linkedin.datahub.upgrade.UpgradeCleanupStep;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.metadata.entity.ebean.EbeanEntityService;
import com.linkedin.metadata.graph.Neo4jGraphClient;
import com.linkedin.metadata.models.registry.SnapshotEntityRegistry;
import io.ebean.EbeanServer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;


public class NoCodeCleanupUpgrade implements Upgrade {

  private final List<UpgradeStep<?>> _steps;
  private final List<UpgradeCleanupStep> _cleanupSteps;

  // Upgrade requires the EbeanServer.
  public NoCodeCleanupUpgrade(final EbeanServer server, final EbeanEntityService entityService,
      final SnapshotEntityRegistry entityRegistry, Neo4jGraphClient graphClient) {
    _steps = buildUpgradeSteps(
        server,
        entityService,
        entityRegistry,
        graphClient);
    _cleanupSteps = buildCleanupSteps(server);
  }

  @Override
  public String id() {
    return "NoCodeDataMigrationCleanup";
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
    return Collections.emptyList();
  }

  private List<UpgradeStep<?>> buildUpgradeSteps(
      final EbeanServer server,
      final EbeanEntityService entityService,
      final SnapshotEntityRegistry entityRegistry, Neo4jGraphClient graphClient) {
    final List<UpgradeStep<?>> steps = new ArrayList<>();
    steps.add(new NoCodeUpgradeQualificationStep(server));
    steps.add(new DeleteAspectTableStep(server));
    steps.add(new DeleteLegacyGraphRelationshipsStep(graphClient));
    return steps;
  }
}
