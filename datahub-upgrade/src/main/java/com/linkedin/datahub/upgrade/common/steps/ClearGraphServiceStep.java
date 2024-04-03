package com.linkedin.datahub.upgrade.common.steps;

import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeStepResult;
import com.linkedin.datahub.upgrade.nocode.NoCodeUpgrade;
import com.linkedin.metadata.graph.GraphService;
import java.util.function.Function;

public class ClearGraphServiceStep implements UpgradeStep {

  private final String deletePattern = ".*";

  private final GraphService _graphService;
  private final boolean _alwaysRun;

  public ClearGraphServiceStep(final GraphService graphService, final boolean alwaysRun) {
    _graphService = graphService;
    _alwaysRun = alwaysRun;
  }

  @Override
  public String id() {
    return "ClearGraphServiceStep";
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
        _graphService.clear();
      } catch (Exception e) {
        context.report().addLine("Failed to clear graph indices", e);
        return new DefaultUpgradeStepResult(id(), UpgradeStepResult.Result.FAILED);
      }
      return new DefaultUpgradeStepResult(id(), UpgradeStepResult.Result.SUCCEEDED);
    };
  }
}
