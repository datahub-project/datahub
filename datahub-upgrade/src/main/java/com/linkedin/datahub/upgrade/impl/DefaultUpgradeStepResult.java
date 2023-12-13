package com.linkedin.datahub.upgrade.impl;

import com.linkedin.datahub.upgrade.UpgradeStepResult;

public class DefaultUpgradeStepResult implements UpgradeStepResult {

  private final String _stepId;
  private final Result _result;
  private final Action _action;

  public DefaultUpgradeStepResult(String stepId, Result result) {
    this(stepId, result, Action.CONTINUE);
  }

  public DefaultUpgradeStepResult(String stepId, Result result, Action action) {
    _stepId = stepId;
    _result = result;
    _action = action;
  }

  @Override
  public String stepId() {
    return _stepId;
  }

  @Override
  public Result result() {
    return _result;
  }

  @Override
  public Action action() {
    return _action;
  }
}
