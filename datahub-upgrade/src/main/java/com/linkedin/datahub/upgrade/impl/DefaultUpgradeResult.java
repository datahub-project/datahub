package com.linkedin.datahub.upgrade.impl;

import com.linkedin.datahub.upgrade.UpgradeReport;
import com.linkedin.datahub.upgrade.UpgradeResult;

public class DefaultUpgradeResult implements UpgradeResult {

  private final Result _result;
  private final UpgradeReport _report;

  DefaultUpgradeResult(Result result, UpgradeReport report) {
    _result = result;
    _report = report;
  }

  @Override
  public Result result() {
    return _result;
  }

  @Override
  public UpgradeReport report() {
    return _report;
  }
}
