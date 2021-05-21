package com.linkedin.datahub.upgrade;

import java.util.List;


/**
 * Context about a currently running upgrade.
 */
public interface UpgradeContext {

  /**
   * Returns the currently running upgrade.
   */
  Upgrade upgrade();

  /**
   * Returns the results from steps that have been completed.
   */
  List<UpgradeStepResult<?>> stepResults();

  /**
   * Returns a report object where human-readable messages can be logged.
   */
  UpgradeReport report();

  /**
   * Returns a list of arguments that have been provided as input to the upgrade.
   */
  List<String> args();

}
