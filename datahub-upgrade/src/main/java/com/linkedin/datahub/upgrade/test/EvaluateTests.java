package com.linkedin.datahub.upgrade.test;

import com.datahub.authentication.Authentication;
import com.google.common.collect.ImmutableList;
import com.linkedin.datahub.upgrade.Upgrade;
import com.linkedin.datahub.upgrade.UpgradeCleanupStep;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.test.TestEngine;
import java.util.ArrayList;
import java.util.List;


/**
 * This upgrade evaluates tests for all entities
 */
public class EvaluateTests implements Upgrade {

  public static final String ELASTIC_TIMEOUT_ENV_NAME = "METADATA_TEST_ELASTIC_TIMEOUT";
  public static final String EXECUTOR_POOL_SIZE = "EXECUTOR_POOL_SIZE";

  private final List<UpgradeStep> _steps;

  public EvaluateTests(
      final EntityClient entityClient,
      final EntitySearchService entitySearchService,
      final TestEngine testEngine,
      final Authentication systemAuthentication) {
    _steps = buildSteps(
        entityClient,
        entitySearchService,
        testEngine,
        systemAuthentication);
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
      final TestEngine testEngine,
      final Authentication systemAuthentication) {
    final List<UpgradeStep> steps = new ArrayList<>();
    steps.add(new EvaluateTestsStep(entityClient, entitySearchService, testEngine, systemAuthentication));
    return steps;
  }

  @Override
  public List<UpgradeCleanupStep> cleanupSteps() {
    return ImmutableList.of();
  }
}

