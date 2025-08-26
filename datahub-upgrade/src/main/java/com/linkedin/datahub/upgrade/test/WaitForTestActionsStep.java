package com.linkedin.datahub.upgrade.test;

import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeStepResult;
import com.linkedin.metadata.test.TestEngine;
import com.linkedin.upgrade.DataHubUpgradeState;
import java.util.function.Function;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class WaitForTestActionsStep implements UpgradeStep {
  private final TestEngine testEngine;

  public WaitForTestActionsStep(@Nonnull TestEngine testEngine) {
    this.testEngine = testEngine;
  }

  @Override
  public String id() {
    return "WaitForTestActions";
  }

  @Override
  public Function<UpgradeContext, UpgradeStepResult> executable() {
    return (context) -> {
      context.report().addLine("Waiting for test actions to complete.");
      testEngine.close();
      context.report().addLine("Test actions successfully completed.");
      return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.SUCCEEDED);
    };
  }
}
