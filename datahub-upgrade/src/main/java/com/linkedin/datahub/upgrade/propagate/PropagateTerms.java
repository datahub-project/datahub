package com.linkedin.datahub.upgrade.propagate;

import com.google.common.collect.ImmutableList;
import com.linkedin.datahub.upgrade.Upgrade;
import com.linkedin.datahub.upgrade.UpgradeCleanupStep;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.search.EntitySearchService;
import java.util.ArrayList;
import java.util.List;


/**
 * This job propagates the terms of a predefined set of datasets to all other datasets which have similar schema to the
 * source dataset
 */
public class PropagateTerms implements Upgrade {

  private final List<UpgradeStep> _steps;

  public PropagateTerms(final EntityService entityService, final EntitySearchService entitySearchService) {
    _steps = buildSteps(entityService, entitySearchService);
  }

  @Override
  public String id() {
    return "PropagateTerms";
  }

  @Override
  public List<UpgradeStep> steps() {
    return _steps;
  }

  private List<UpgradeStep> buildSteps(final EntityService entityService,
      final EntitySearchService entitySearchService) {
    final List<UpgradeStep> steps = new ArrayList<>();
    steps.add(new PropagateTermsStep(entityService, entitySearchService));
    return steps;
  }

  @Override
  public List<UpgradeCleanupStep> cleanupSteps() {
    return ImmutableList.of();
  }
}

