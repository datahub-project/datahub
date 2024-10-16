package com.linkedin.datahub.upgrade.impl;

import com.linkedin.datahub.upgrade.UpgradeReport;
import com.linkedin.datahub.upgrade.UpgradeResult;
import com.linkedin.upgrade.DataHubUpgradeState;

public class DefaultUpgradeResult implements UpgradeResult {

  private final DataHubUpgradeState _result;
  private final UpgradeReport _report;

  DefaultUpgradeResult(DataHubUpgradeState result, UpgradeReport report) {
    _result = result;
    _report = report;
  }

  @Override
  public DataHubUpgradeState result() {
    return _result;
  }

  @Override
  public UpgradeReport report() {
    return _report;
  }
}
