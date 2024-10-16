package com.linkedin.datahub.upgrade.nocode;

import com.google.common.collect.ImmutableMap;
import com.linkedin.datahub.upgrade.Upgrade;
import com.linkedin.datahub.upgrade.UpgradeCleanupStep;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.common.steps.GMSEnableWriteModeStep;
import com.linkedin.datahub.upgrade.common.steps.GMSQualificationStep;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.models.registry.EntityRegistry;
import io.ebean.Database;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;

public class NoCodeUpgrade implements Upgrade {

  public static final String BATCH_SIZE_ARG_NAME = "batchSize";
  public static final String BATCH_DELAY_MS_ARG_NAME = "batchDelayMs";
  public static final String FORCE_UPGRADE_ARG_NAME = "force-upgrade";
  public static final String CLEAN_ARG_NAME = "clean";

  private final List<UpgradeStep> _steps;
  private final List<UpgradeCleanupStep> _cleanupSteps;

  // Upgrade requires the Database.
  public NoCodeUpgrade(
      @Nullable final Database server,
      final EntityService<?> entityService,
      final EntityRegistry entityRegistry,
      final SystemEntityClient entityClient) {
    if (server != null) {
      _steps = buildUpgradeSteps(server, entityService, entityRegistry, entityClient);
      _cleanupSteps = buildCleanupSteps();
    } else {
      _steps = List.of();
      _cleanupSteps = List.of();
    }
  }

  @Override
  public String id() {
    return "NoCodeDataMigration";
  }

  @Override
  public List<UpgradeStep> steps() {
    return _steps;
  }

  @Override
  public List<UpgradeCleanupStep> cleanupSteps() {
    return _cleanupSteps;
  }

  private List<UpgradeCleanupStep> buildCleanupSteps() {
    return Collections.emptyList();
  }

  private List<UpgradeStep> buildUpgradeSteps(
      final Database server,
      final EntityService<?> entityService,
      final EntityRegistry entityRegistry,
      final SystemEntityClient entityClient) {
    final List<UpgradeStep> steps = new ArrayList<>();
    steps.add(new RemoveAspectV2TableStep(server));
    steps.add(new GMSQualificationStep(ImmutableMap.of("noCode", "true")));
    steps.add(new UpgradeQualificationStep(server));
    steps.add(new CreateAspectTableStep(server));
    steps.add(new DataMigrationStep(server, entityService, entityRegistry));
    steps.add(new GMSEnableWriteModeStep(entityClient));
    return steps;
  }
}
