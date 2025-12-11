/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.datahub.upgrade;

import io.datahubproject.metadata.context.OperationContext;
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

  /** Returns the operation context for the upgrade */
  OperationContext opContext();
}
