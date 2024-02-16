package com.linkedin.datahub.upgrade.system.entity.steps;

import com.google.common.collect.ImmutableList;
import com.linkedin.datahub.upgrade.Upgrade;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.search.SearchService;
import java.util.List;

public class BackfillPolicyFields implements Upgrade {
  private final List<UpgradeStep> _steps;

  public BackfillPolicyFields(
      EntityService<?> entityService,
      SearchService searchService,
      boolean enabled,
      boolean reprocessEnabled,
      Integer batchSize) {
    if (enabled) {
      _steps =
          ImmutableList.of(
              new BackfillPolicyFieldsStep(
                  entityService, searchService, reprocessEnabled, batchSize));
    } else {
      _steps = ImmutableList.of();
    }
  }

  @Override
  public String id() {
    return "BackfillPolicyField";
  }

  @Override
  public List<UpgradeStep> steps() {
    return _steps;
  }
}
