package com.linkedin.datahub.upgrade.system.restoreindices.forminfo;

import com.google.common.collect.ImmutableList;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.system.NonBlockingSystemUpgrade;
import com.linkedin.metadata.entity.EntityService;
import java.util.List;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RestoreFormInfoIndices implements NonBlockingSystemUpgrade {

  private final List<UpgradeStep> _steps;

  public RestoreFormInfoIndices(
      @Nonnull final EntityService<?> entityService, final boolean enabled) {
    _steps =
        enabled
            ? ImmutableList.of(new RestoreFormInfoIndicesStep(entityService))
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
