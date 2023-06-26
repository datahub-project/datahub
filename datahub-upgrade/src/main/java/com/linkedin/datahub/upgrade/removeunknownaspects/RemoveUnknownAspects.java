package com.linkedin.datahub.upgrade.removeunknownaspects;

import com.google.common.collect.ImmutableList;
import com.linkedin.datahub.upgrade.Upgrade;
import com.linkedin.datahub.upgrade.UpgradeCleanupStep;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.metadata.entity.EntityServiceImpl;
import java.util.ArrayList;
import java.util.List;


public class RemoveUnknownAspects implements Upgrade {

  private final List<UpgradeStep> _steps;

  public RemoveUnknownAspects(final EntityServiceImpl entityServiceImpl) {
    _steps = buildSteps(entityServiceImpl);
  }

  @Override
  public String id() {
    return this.getClass().getSimpleName();
  }

  @Override
  public List<UpgradeStep> steps() {
    return _steps;
  }

  private List<UpgradeStep> buildSteps(final EntityServiceImpl entityServiceImpl) {
    final List<UpgradeStep> steps = new ArrayList<>();
    steps.add(new RemoveClientIdAspectStep(entityServiceImpl));
    return steps;
  }

  @Override
  public List<UpgradeCleanupStep> cleanupSteps() {
    return ImmutableList.of();
  }
}
