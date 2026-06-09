package com.linkedin.datahub.upgrade.system.restoreindices.columnlineage;

import com.google.common.collect.ImmutableList;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.system.NonBlockingSystemUpgrade;
import com.linkedin.metadata.entity.EntityService;
import java.util.List;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RestoreColumnLineageIndices implements NonBlockingSystemUpgrade {

  private final List<UpgradeStep> _steps;

  public RestoreColumnLineageIndices(
      @Nonnull final EntityService<?> entityService, final boolean enabled) {
    _steps =
        enabled
            ? ImmutableList.of(new RestoreColumnLineageIndicesStep(entityService))
            : ImmutableList.of();
  }

  @Override
  public String id() {
    return this.getClass().getName();
  }

  @Override
  public List<UpgradeStep> steps() {
    return _steps;
  }
}
