package com.linkedin.datahub.upgrade.restoreindices;

import com.google.common.collect.ImmutableList;
import com.linkedin.datahub.upgrade.Upgrade;
import com.linkedin.datahub.upgrade.UpgradeCleanupStep;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.commonsteps.GMSQualificationStep;
import com.linkedin.datahub.upgrade.commonsteps.MAEQualificationStep;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.models.registry.EntityRegistry;
import io.ebean.EbeanServer;
import java.util.ArrayList;
import java.util.List;


public class RestoreIndices implements Upgrade {
  public static final String BATCH_SIZE_ARG_NAME = "batchSize";
  public static final String BATCH_DELAY_MS_ARG_NAME = "batchDelayMs";

  private final List<UpgradeStep> _steps;

  public RestoreIndices(final EbeanServer server, final EntityService entityService, final EntityRegistry entityRegistry) {
    _steps = buildSteps(server, entityService, entityRegistry);
  }

  @Override
  public String id() {
    return "RestoreIndices";
  }

  @Override
  public List<UpgradeStep> steps() {
    return _steps;
  }

  private List<UpgradeStep> buildSteps(final EbeanServer server, final EntityService entityService,
      final EntityRegistry entityRegistry) {
    final List<UpgradeStep> steps = new ArrayList<>();
    steps.add(new GMSQualificationStep());
    steps.add(new MAEQualificationStep());
    steps.add(new SendMAEStep(server, entityService, entityRegistry));
    return steps;
  }

  @Override
  public List<UpgradeCleanupStep> cleanupSteps() {
    return ImmutableList.of();
  }
}
