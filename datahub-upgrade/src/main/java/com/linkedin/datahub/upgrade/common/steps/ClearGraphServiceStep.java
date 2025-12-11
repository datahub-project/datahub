/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.datahub.upgrade.common.steps;

import static com.linkedin.datahub.upgrade.common.Constants.CLEAN_ARG_NAME;

import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeStepResult;
import com.linkedin.metadata.graph.GraphService;
import com.linkedin.upgrade.DataHubUpgradeState;
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
    if (context.parsedArgs().containsKey(CLEAN_ARG_NAME)) {
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
        return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.FAILED);
      }
      return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.SUCCEEDED);
    };
  }
}
