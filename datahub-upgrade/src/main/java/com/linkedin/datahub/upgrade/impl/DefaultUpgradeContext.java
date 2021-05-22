package com.linkedin.datahub.upgrade.impl;

import com.linkedin.datahub.upgrade.Upgrade;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeReport;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import java.util.List;


public class DefaultUpgradeContext implements UpgradeContext {

  private final Upgrade _upgrade;
  private final UpgradeReport _report;
  private final List<UpgradeStepResult<?>> _previousStepResults;
  private final List<String> _args;

  DefaultUpgradeContext(
      Upgrade upgrade,
      UpgradeReport report,
      List<UpgradeStepResult<?>> previousStepResults,
      List<String> args) {
    _upgrade = upgrade;
    _report = report;
    _previousStepResults = previousStepResults;
    _args = args;
  }

  @Override
  public Upgrade upgrade() {
    return _upgrade;
  }

  @Override
  public List<UpgradeStepResult<?>> stepResults() {
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
}
