package com.linkedin.datahub.upgrade;

public interface UpgradeStepResult {

  /** Returns a string identifier associated with the step. */
  String stepId();

  /** The outcome of the step execution. */
  enum Result {
    /** The step succeeded. */
    SUCCEEDED,
    /** The step failed. */
    FAILED
  }

  /** A control-flow action to perform as a result of the step execution. */
  enum Action {
    /** Continue attempting the upgrade. */
    CONTINUE,
    /** Immediately fail the upgrade, without retry. */
    FAIL,
    /** Immediately abort the upgrade, without retry. */
    ABORT
  }

  /** Returns the result of executing the step, either success or failure. */
  Result result();

  /** Returns the action to perform after executing the step, either continue or abort. */
  default Action action() {
    return Action.CONTINUE;
  }
  ;
}
