package com.linkedin.datahub.upgrade.system.executorpools;

import com.google.common.collect.ImmutableList;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.system.BlockingSystemUpgrade;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.search.SearchService;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;

public class ExecutorPools implements BlockingSystemUpgrade {

  private final List<UpgradeStep> _steps;

  public ExecutorPools(
      OperationContext opContext,
      EntityService<?> entityService,
      SearchService searchService,
      boolean enabled,
      boolean reprocessEnabled,
      Integer batchSize,
      String customerId) {
    if (enabled) {
      _steps =
          ImmutableList.of(
              new ExecutorPoolsStep(
                  opContext,
                  entityService,
                  searchService,
                  enabled,
                  reprocessEnabled,
                  batchSize,
                  customerId));
    } else {
      _steps = ImmutableList.of();
    }
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
