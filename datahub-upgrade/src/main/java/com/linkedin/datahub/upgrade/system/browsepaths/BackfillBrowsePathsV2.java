package com.linkedin.datahub.upgrade.system.browsepaths;

import com.google.common.collect.ImmutableList;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.system.NonBlockingSystemUpgrade;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.search.SearchService;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;

public class BackfillBrowsePathsV2 implements NonBlockingSystemUpgrade {

  private final List<UpgradeStep> _steps;

  public BackfillBrowsePathsV2(
      OperationContext opContext,
      EntityService<?> entityService,
      SearchService searchService,
      boolean enabled,
      boolean reprocessEnabled,
      Integer batchSize) {
    if (enabled) {
      _steps =
          ImmutableList.of(
              new BackfillBrowsePathsV2Step(
                  opContext, entityService, searchService, reprocessEnabled, batchSize));
    } else {
      _steps = ImmutableList.of();
    }
  }

  @Override
  public String id() {
    return "BackfillBrowsePathsV2";
  }

  @Override
  public List<UpgradeStep> steps() {
    return _steps;
  }
}
