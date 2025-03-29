package com.linkedin.datahub.upgrade.system.executors;

import com.google.common.collect.ImmutableList;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.system.BlockingSystemUpgrade;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.search.SearchService;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class PurgeLegacyExecutors implements BlockingSystemUpgrade {

  private List<UpgradeStep> _steps;

  public PurgeLegacyExecutors(
      OperationContext opContext,
      EntityService<?> entityService,
      SearchService searchService,
      boolean enabled,
      boolean reprocessEnabled,
      Integer batchSize) {
    _steps = ImmutableList.of();

    if (enabled) {
      _steps =
          ImmutableList.of(
              new PurgeLegacyExecutorsStep(
                  opContext, entityService, searchService, enabled, reprocessEnabled, batchSize));
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
