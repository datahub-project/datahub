package com.linkedin.datahub.upgrade.system.restoreindices.assertions;

import com.google.common.collect.ImmutableList;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.system.NonBlockingSystemUpgrade;
import com.linkedin.metadata.entity.AspectDao;
import com.linkedin.metadata.entity.EntityService;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

/** A job that reindexes all assertion info aspects to fix the type searchable fields. */
@Slf4j
public class ReindexAssertionInfo implements NonBlockingSystemUpgrade {

  private final List<UpgradeStep> _steps;

  public ReindexAssertionInfo(
      @Nonnull OperationContext opContext,
      EntityService<?> entityService,
      AspectDao aspectDao,
      boolean enabled,
      Integer batchSize,
      Integer batchDelayMs,
      Integer limit) {
    if (enabled) {
      _steps =
          ImmutableList.of(
              new ReindexAssertionInfoStep(
                  opContext, entityService, aspectDao, batchSize, batchDelayMs, limit));
    } else {
      _steps = ImmutableList.of();
    }
  }

  @Override
  public String id() {
    return this.getClass().getName() + "v3";
  }

  @Override
  public List<UpgradeStep> steps() {
    return _steps;
  }
}
