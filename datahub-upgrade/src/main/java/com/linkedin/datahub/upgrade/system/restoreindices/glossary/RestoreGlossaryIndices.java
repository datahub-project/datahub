package com.linkedin.datahub.upgrade.system.restoreindices.glossary;

import com.google.common.collect.ImmutableList;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.system.NonBlockingSystemUpgrade;
import com.linkedin.metadata.entity.AspectDao;
import com.linkedin.metadata.entity.EntityService;
import java.util.List;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RestoreGlossaryIndices implements NonBlockingSystemUpgrade {

  private final List<UpgradeStep> _steps;

  public RestoreGlossaryIndices(
      @Nonnull final EntityService<?> entityService,
      @Nonnull final AspectDao aspectDao,
      final boolean enabled) {
    _steps =
        enabled
            ? ImmutableList.of(new RestoreGlossaryIndicesStep(entityService, aspectDao))
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
