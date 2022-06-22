package com.linkedin.datahub.upgrade.test;

import com.google.common.collect.ImmutableList;
import com.linkedin.datahub.upgrade.Upgrade;
import com.linkedin.datahub.upgrade.UpgradeCleanupStep;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.test.TestEngine;
import java.util.ArrayList;
import java.util.List;


/**
 * This upgrade evaluates tests for all entities
 */
public class EvaluateTests implements Upgrade {

  private final List<UpgradeStep> _steps;

  public EvaluateTests(final EntitySearchService entitySearchService, final TestEngine testEngine) {
    _steps = buildSteps(entitySearchService, testEngine);
  }

  @Override
  public String id() {
    return "EvaluateTests";
  }

  @Override
  public List<UpgradeStep> steps() {
    return _steps;
  }

  private List<UpgradeStep> buildSteps(final EntitySearchService entitySearchService, final TestEngine testEngine) {
    final List<UpgradeStep> steps = new ArrayList<>();
    steps.add(new EvaluateTestsStep(entitySearchService, testEngine));
    return steps;
  }

  @Override
  public List<UpgradeCleanupStep> cleanupSteps() {
    return ImmutableList.of();
  }
}

