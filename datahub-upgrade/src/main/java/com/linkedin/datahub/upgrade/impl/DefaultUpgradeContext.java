package com.linkedin.datahub.upgrade.impl;

import com.linkedin.datahub.upgrade.Upgrade;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeReport;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.UpgradeUtils;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nonnull;
import lombok.Getter;
import lombok.experimental.Accessors;

@Getter
@Accessors(fluent = true)
public class DefaultUpgradeContext implements UpgradeContext {

  private final OperationContext opContext;
  private final Upgrade upgrade;
  private final UpgradeReport report;
  private final List<UpgradeStepResult> previousStepResults;
  private final List<String> args;
  private final Map<String, Optional<String>> parsedArgs;

  public DefaultUpgradeContext(
      @Nonnull OperationContext opContext,
      Upgrade upgrade,
      UpgradeReport report,
      List<UpgradeStepResult> previousStepResults,
      List<String> args) {
    this.opContext = opContext;
    this.upgrade = upgrade;
    this.report = report;
    this.previousStepResults = previousStepResults;
    this.args = args;
    this.parsedArgs = UpgradeUtils.parseArgs(args);
  }

  @Override
  public List<UpgradeStepResult> stepResults() {
    return previousStepResults;
  }
}
