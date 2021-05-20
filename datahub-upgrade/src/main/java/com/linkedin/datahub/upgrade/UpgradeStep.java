package com.linkedin.datahub.upgrade;

import java.util.function.Function;


public interface UpgradeStep<T> {

  String id();

  /**
   * Returns a function representing the step's execution logic.
   */
  Function<UpgradeContext, UpgradeStepResult<T>> executable();

  /**
   * Returns the number of times the step should be retried.
   */
  default int retryCount() {
    return 0;
  }

  /**
   * Returns whether the upgrade should proceed if the step fails after exceeding the maximum retries.
   */
  default boolean isOptional() {
    return false;
  }

}
