package com.linkedin.datahub.upgrade.impl;

import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.upgrade.DataHubUpgradeState;

public class DefaultUpgradeStepResult implements UpgradeStepResult {

  private final String _stepId;
  private final DataHubUpgradeState _result;
  private final Action _action;

  public DefaultUpgradeStepResult(String stepId, DataHubUpgradeState result) {
    this(stepId, result, Action.CONTINUE);
  }

  public DefaultUpgradeStepResult(String stepId, DataHubUpgradeState result, Action action) {
    _stepId = stepId;
    _result = result;
    _action = action;
  }

  @Override
  public String stepId() {
    return _stepId;
  }

  @Override
  public DataHubUpgradeState result() {
    return _result;
  }

  @Override
  public Action action() {
    return _action;
  }
}
