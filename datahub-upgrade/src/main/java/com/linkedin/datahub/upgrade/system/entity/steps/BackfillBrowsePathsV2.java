package com.linkedin.datahub.upgrade.system.entity.steps;

import com.google.common.collect.ImmutableList;
import com.linkedin.datahub.upgrade.Upgrade;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.search.SearchService;
import java.util.List;

public class BackfillBrowsePathsV2 implements Upgrade {

  private final List<UpgradeStep> _steps;

  public BackfillBrowsePathsV2(EntityService entityService, SearchService searchService) {
    _steps = ImmutableList.of(new BackfillBrowsePathsV2Step(entityService, searchService));
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
