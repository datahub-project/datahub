package com.linkedin.datahub.upgrade.impl;

import com.linkedin.datahub.upgrade.Upgrade;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeReport;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.UpgradeUtils;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.Getter;
import lombok.experimental.Accessors;

@Getter
@Accessors(fluent = true)
public class DefaultUpgradeContext implements UpgradeContext {

  private final Upgrade upgrade;
  private final UpgradeReport report;
  private final List<UpgradeStepResult> previousStepResults;
  private final List<String> args;
  private final Map<String, Optional<String>> parsedArgs;

  DefaultUpgradeContext(
      Upgrade upgrade,
      UpgradeReport report,
      List<UpgradeStepResult> previousStepResults,
      List<String> args) {
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
