package com.linkedin.datahub.upgrade;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Context about a currently running upgrade. */
public interface UpgradeContext {

  /** Returns the currently running upgrade. */
  Upgrade upgrade();

  /** Returns the results from steps that have been completed. */
  List<UpgradeStepResult> stepResults();

  /** Returns a report object where human-readable messages can be logged. */
  UpgradeReport report();

  /** Returns a list of raw arguments that have been provided as input to the upgrade. */
  List<String> args();

  /** Returns a map of argument to <>optional</> value, as delimited by an '=' character. */
  Map<String, Optional<String>> parsedArgs();
}
