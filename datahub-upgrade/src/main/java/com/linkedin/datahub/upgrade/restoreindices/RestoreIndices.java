package com.linkedin.datahub.upgrade.restoreindices;

import com.google.common.collect.ImmutableList;
import com.linkedin.datahub.upgrade.Upgrade;
import com.linkedin.datahub.upgrade.UpgradeCleanupStep;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.common.steps.ClearGraphServiceStep;
import com.linkedin.datahub.upgrade.common.steps.ClearSearchServiceStep;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.search.EntitySearchService;
import io.ebean.EbeanServer;
import java.util.ArrayList;
import java.util.List;


public class RestoreIndices implements Upgrade {
  public static final String BATCH_SIZE_ARG_NAME = "batchSize";
  public static final String BATCH_DELAY_MS_ARG_NAME = "batchDelayMs";
  public static final String NUM_THREADS_ARG_NAME = "numThreads";
  public static final String ASPECT_NAME_ARG_NAME = "aspectName";
  public static final String URN_ARG_NAME = "urn";

  private final List<UpgradeStep> _steps;

  public RestoreIndices(final EbeanServer server, final EntityService entityService,
      final EntityRegistry entityRegistry, final EntitySearchService entitySearchService,
      final GraphService graphService) {
    _steps = buildSteps(server, entityService, entityRegistry, entitySearchService, graphService);
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
      final EntityRegistry entityRegistry, final EntitySearchService entitySearchService,
      final GraphService graphService) {
    final List<UpgradeStep> steps = new ArrayList<>();
    steps.add(new ClearSearchServiceStep(entitySearchService, false));
    steps.add(new ClearGraphServiceStep(graphService, false));
    steps.add(new SendMAEStep(server, entityService, entityRegistry));
    return steps;
  }

  @Override
  public List<UpgradeCleanupStep> cleanupSteps() {
    return ImmutableList.of();
  }
}
