package com.linkedin.datahub.upgrade.common.steps;

import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeStepResult;
import com.linkedin.datahub.upgrade.nocode.NoCodeUpgrade;
import com.linkedin.metadata.search.EntitySearchService;
import java.util.function.Function;

public class ClearSearchServiceStep implements UpgradeStep {

  private final EntitySearchService _entitySearchService;
  private final boolean _alwaysRun;

  public ClearSearchServiceStep(
      final EntitySearchService entitySearchService, final boolean alwaysRun) {
    _entitySearchService = entitySearchService;
    _alwaysRun = alwaysRun;
  }

  @Override
  public String id() {
    return "ClearSearchServiceStep";
  }

  @Override
  public boolean skip(UpgradeContext context) {
    if (_alwaysRun) {
      return false;
    }
    if (context.parsedArgs().containsKey(NoCodeUpgrade.CLEAN_ARG_NAME)) {
      return false;
    }
    context.report().addLine("Cleanup has not been requested.");
    return true;
  }

  @Override
  public int retryCount() {
    return 1;
  }

  @Override
  public Function<UpgradeContext, UpgradeStepResult> executable() {
    return (context) -> {
      try {
        _entitySearchService.clear();
      } catch (Exception e) {
        context.report().addLine("Failed to clear search service", e);
        return new DefaultUpgradeStepResult(id(), UpgradeStepResult.Result.FAILED);
      }
      return new DefaultUpgradeStepResult(id(), UpgradeStepResult.Result.SUCCEEDED);
    };
  }
}
