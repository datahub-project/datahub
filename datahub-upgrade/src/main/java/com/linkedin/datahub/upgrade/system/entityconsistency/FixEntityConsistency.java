package com.linkedin.datahub.upgrade.system.entityconsistency;

import com.google.common.collect.ImmutableList;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.system.NonBlockingSystemUpgrade;
import com.linkedin.metadata.aspect.consistency.ConsistencyService;
import com.linkedin.metadata.config.EntityConsistencyConfiguration;
import com.linkedin.metadata.entity.EntityService;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

/**
 * A {@link NonBlockingSystemUpgrade} upgrade job that finds and fixes consistency issues.
 *
 * <p>By default, this upgrade runs in dry-run mode, which only logs what would be changed without
 * making actual modifications. Set dryRun=false to apply fixes.
 *
 * <p>Configuration is loaded from {@link EntityConsistencyConfiguration}.
 */
@Slf4j
public class FixEntityConsistency implements NonBlockingSystemUpgrade {

  private final List<UpgradeStep> _steps;

  public FixEntityConsistency(
      @Nonnull OperationContext opContext,
      @Nonnull EntityService<?> entityService,
      @Nonnull ConsistencyService consistencyService,
      @Nonnull EntityConsistencyConfiguration config) {
    if (config.isEnabled()) {
      _steps =
          ImmutableList.of(
              new FixEntityConsistencyStep(opContext, entityService, consistencyService, config));
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
