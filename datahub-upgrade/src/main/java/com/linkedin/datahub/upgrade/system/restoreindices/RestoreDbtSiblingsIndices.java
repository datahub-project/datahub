package com.linkedin.datahub.upgrade.system.restoreindices;

import com.google.common.collect.ImmutableList;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.system.NonBlockingSystemUpgrade;
import com.linkedin.metadata.entity.EntityService;
import java.util.List;
import javax.annotation.Nonnull;

public class RestoreDbtSiblingsIndices implements NonBlockingSystemUpgrade {

  private final List<UpgradeStep> _steps;

  public RestoreDbtSiblingsIndices(
      @Nonnull final EntityService<?> entityService, final boolean enabled) {
    _steps = ImmutableList.of(new RestoreDbtSiblingsIndicesStep(entityService, enabled));
  }

  @Override
  public String id() {
    return getClass().getSimpleName();
  }

  @Override
  public List<UpgradeStep> steps() {
    return _steps;
  }
}
