package com.linkedin.datahub.upgrade.common.steps;

import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeStepResult;
import com.linkedin.datahub.upgrade.nocode.NoCodeUpgrade;
import com.linkedin.metadata.systemmetadata.SystemMetadataService;
import com.linkedin.upgrade.DataHubUpgradeState;
import java.util.function.Function;

public class ClearSystemMetadataServiceStep implements UpgradeStep {

  private final SystemMetadataService _systemMetadataService;
  private final boolean _alwaysRun;

  public ClearSystemMetadataServiceStep(
      final SystemMetadataService systemMetadataService, final boolean alwaysRun) {
    _systemMetadataService = systemMetadataService;
    _alwaysRun = alwaysRun;
  }

  @Override
  public String id() {
    return "ClearSystemMetadataServiceStep";
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
        _systemMetadataService.clear();
      } catch (Exception e) {
        context.report().addLine("Failed to clear system metadata service", e);
        return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.FAILED);
      }
      return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.SUCCEEDED);
    };
  }
}
