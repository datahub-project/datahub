package com.linkedin.datahub.upgrade.system.ingestionrecipes;

import com.google.common.collect.ImmutableList;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.system.NonBlockingSystemUpgrade;
import com.linkedin.metadata.entity.EntityService;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

/**
 * A job that reindexes all domain aspects as part of reindexing descriptions This is required to
 * fix the analytics for domains
 */
@Slf4j
public class DisableUrnLowercasing implements NonBlockingSystemUpgrade {

  private final List<UpgradeStep> _steps;

  public DisableUrnLowercasing(
      @Nonnull OperationContext opContext, EntityService<?> entityService, boolean enabled) {
    if (enabled) {
      _steps = ImmutableList.of(new DisableUrnLowercasingStep(opContext, entityService));
    } else {
      _steps = ImmutableList.of();
    }
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
