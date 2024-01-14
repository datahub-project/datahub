package com.linkedin.datahub.upgrade.impl;

import com.linkedin.datahub.upgrade.Upgrade;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeReport;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.UpgradeUtils;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class DefaultUpgradeContext implements UpgradeContext {

  private final Upgrade _upgrade;
  private final UpgradeReport _report;
  private final List<UpgradeStepResult> _previousStepResults;
  private final List<String> _args;
  private final Map<String, Optional<String>> _parsedArgs;

  DefaultUpgradeContext(
      Upgrade upgrade,
      UpgradeReport report,
      List<UpgradeStepResult> previousStepResults,
      List<String> args) {
    _upgrade = upgrade;
    _report = report;
    _previousStepResults = previousStepResults;
    _args = args;
    _parsedArgs = UpgradeUtils.parseArgs(args);
  }

  @Override
  public Upgrade upgrade() {
    return _upgrade;
  }

  @Override
  public List<UpgradeStepResult> stepResults() {
    return _previousStepResults;
  }

  @Override
  public UpgradeReport report() {
    return _report;
  }

  @Override
  public List<String> args() {
    return _args;
  }

  @Override
  public Map<String, Optional<String>> parsedArgs() {
    return _parsedArgs;
  }
}
