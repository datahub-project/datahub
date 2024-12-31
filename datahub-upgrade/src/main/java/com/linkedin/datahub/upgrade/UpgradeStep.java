package com.linkedin.datahub.upgrade;

import java.util.function.Function;

/** Represents a single executable step in an {@link Upgrade}. */
public interface UpgradeStep {

  /** Returns an identifier for the upgrade step. */
  String id();

  /** Returns a function representing the step's execution logic. */
  Function<UpgradeContext, UpgradeStepResult> executable();

  /** Returns the number of times the step should be retried. */
  default int retryCount() {
    return 0;
  }

  /**
   * Returns whether the upgrade should proceed if the step fails after exceeding the maximum
   * retries.
   */
  default boolean isOptional() {
    return false;
  }

  /** Returns whether or not to skip the step based on the UpgradeContext */
  default boolean skip(UpgradeContext context) {
    return false;
  }
}
