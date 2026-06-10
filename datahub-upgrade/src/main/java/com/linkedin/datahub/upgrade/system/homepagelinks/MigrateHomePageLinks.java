package com.linkedin.datahub.upgrade.system.homepagelinks;

import com.google.common.collect.ImmutableList;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.system.NonBlockingSystemUpgrade;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.search.EntitySearchService;
import java.util.List;
import javax.annotation.Nonnull;

public class MigrateHomePageLinks implements NonBlockingSystemUpgrade {

  private final List<UpgradeStep> _steps;

  public MigrateHomePageLinks(
      @Nonnull final EntityService<?> entityService,
      @Nonnull final EntitySearchService entitySearchService,
      final boolean enabled,
      final boolean showHomePageRedesign) {
    _steps =
        (enabled && showHomePageRedesign)
            ? ImmutableList.of(new MigrateHomePageLinksStep(entityService, entitySearchService))
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
