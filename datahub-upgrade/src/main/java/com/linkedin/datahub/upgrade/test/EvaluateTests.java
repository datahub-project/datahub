package com.linkedin.datahub.upgrade.test;

import com.google.common.collect.ImmutableList;
import com.linkedin.datahub.upgrade.Upgrade;
import com.linkedin.datahub.upgrade.UpgradeCleanupStep;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.test.TestEngine;
import io.datahubproject.metadata.context.OperationContext;
import java.util.ArrayList;
import java.util.List;

/** This upgrade evaluates tests for all entities */
public class EvaluateTests implements Upgrade {

  public static final String ELASTIC_TIMEOUT_ENV_NAME = "METADATA_TEST_ELASTIC_TIMEOUT";
  public static final String EXECUTOR_POOL_SIZE = "EXECUTOR_POOL_SIZE";
  public static final String EXECUTOR_QUEUE_SIZE = "EXECUTOR_QUEUE_SIZE";
  public static final String EVALUATION_MODE = "EVALUATION_MODE";

  private final List<UpgradeStep> _steps;
  private final OperationContext systemOpContext;

  public EvaluateTests(
      final OperationContext systemOpContext,
      final SystemEntityClient entityClient,
      final EntitySearchService entitySearchService,
      final TestEngine testEngine) {
    this.systemOpContext = systemOpContext;
    _steps = buildSteps(entityClient, entitySearchService, testEngine);
  }

  @Override
  public String id() {
    return "EvaluateTests";
  }

  @Override
  public List<UpgradeStep> steps() {
    return _steps;
  }

  private List<UpgradeStep> buildSteps(
      final EntityClient entityClient,
      final EntitySearchService entitySearchService,
      final TestEngine testEngine) {
    final List<UpgradeStep> steps = new ArrayList<>();
    steps.add(
        new EvaluateTestsStep(systemOpContext, entityClient, entitySearchService, testEngine));
    return steps;
  }

  @Override
  public List<UpgradeCleanupStep> cleanupSteps() {
    return ImmutableList.of();
  }
}
