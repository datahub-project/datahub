package com.linkedin.datahub.upgrade.system.browsepaths;

import com.google.common.collect.ImmutableList;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.system.NonBlockingSystemUpgrade;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.search.SearchService;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;

public class BackfillIcebergBrowsePathsV2 implements NonBlockingSystemUpgrade {

  private final List<UpgradeStep> _steps;

  public BackfillIcebergBrowsePathsV2(
      OperationContext opContext,
      EntityService<?> entityService,
      SearchService searchService,
      boolean enabled,
      Integer batchSize) {
    if (enabled) {
      _steps =
          ImmutableList.of(
              new BackfillIcebergBrowsePathsV2Step(
                  opContext, entityService, searchService, batchSize));
    } else {
      _steps = ImmutableList.of();
    }
  }

  @Override
  public String id() {
    return "BackfillIcebergBrowsePathsV2";
  }

  @Override
  public List<UpgradeStep> steps() {
    return _steps;
  }
}
