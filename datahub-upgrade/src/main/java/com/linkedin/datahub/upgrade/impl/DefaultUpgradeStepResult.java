package com.linkedin.datahub.upgrade.impl;

import com.linkedin.datahub.upgrade.UpgradeStepResult;
import java.util.Optional;

public class DefaultUpgradeStepResult<T> implements UpgradeStepResult<T> {

  private final String _stepId;
  private final Result _result;
  private final Action _action;
  private final String _message;
  private final T _output;

  public DefaultUpgradeStepResult(String stepId, Result result) {
    this(stepId, result, "");
  }

  public DefaultUpgradeStepResult(String stepId, Result result, Action action) {
    this(stepId, result, action, "");
  }

  public DefaultUpgradeStepResult(String stepId, Result result, String message) {
    this(stepId, result, Action.CONTINUE, message);
  }

  public DefaultUpgradeStepResult(String stepId, Result result, Action action, String message) {
    this(stepId, result, action, message, null);
  }

  public DefaultUpgradeStepResult(String stepId, Result result, Action action, String message, T output) {
    _stepId = stepId;
    _result = result;
    _message = message;
    _action = action;
    _output = output;
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

  @Override
  public String message() {
    return _message;
  }

  @Override
  public Optional<T> output() {
    return Optional.ofNullable(_output);
  }
}
