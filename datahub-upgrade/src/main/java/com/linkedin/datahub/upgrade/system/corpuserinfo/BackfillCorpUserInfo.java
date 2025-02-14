package com.linkedin.datahub.upgrade.system.corpuserinfo;

import com.google.common.collect.ImmutableList;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.system.NonBlockingSystemUpgrade;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.search.SearchService;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;

public class BackfillCorpUserInfo implements NonBlockingSystemUpgrade {

  private final List<UpgradeStep> _steps;

  public BackfillCorpUserInfo(
      OperationContext opContext,
      EntityService<?> entityService,
      SearchService searchService,
      boolean enabled,
      boolean reprocessEnabled,
      Integer batchSize) {
    if (enabled) {
      _steps =
          ImmutableList.of(
              new BackfillCorpUserInfoStep(
                  opContext, entityService, searchService, reprocessEnabled, batchSize));
    } else {
      _steps = ImmutableList.of();
    }
  }

  @Override
  public String id() {
    return "BackfillCorpUserInfo";
  }

  @Override
  public List<UpgradeStep> steps() {
    return _steps;
  }
}
