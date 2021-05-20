package com.linkedin.datahub.upgrade;

import java.util.Optional;


public interface UpgradeStepResult<T> {

  enum Result {
    SUCCEEDED,
    FAILED
  }

  enum Action {
    CONTINUE,
    ABORT
  }

  String stepId();

  Result result();

  Action action();

  String message();

  Optional<T> output();

}
