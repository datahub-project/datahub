package com.linkedin.datahub.upgrade.system.dataplatforms;

import com.google.common.collect.ImmutableList;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.system.NonBlockingSystemUpgrade;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.search.EntitySearchService;
import java.util.List;
import javax.annotation.Nonnull;

public class IndexDataPlatforms implements NonBlockingSystemUpgrade {

  private final List<UpgradeStep> _steps;

  public IndexDataPlatforms(
      @Nonnull final EntityService<?> entityService,
      @Nonnull final EntitySearchService entitySearchService,
      final boolean enabled) {
    _steps =
        enabled
            ? ImmutableList.of(new IndexDataPlatformsStep(entityService, entitySearchService))
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
