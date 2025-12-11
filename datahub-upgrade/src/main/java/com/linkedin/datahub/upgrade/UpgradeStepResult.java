/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.datahub.upgrade;

import com.linkedin.upgrade.DataHubUpgradeState;

public interface UpgradeStepResult {

  /** Returns a string identifier associated with the step. */
  String stepId();

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
  DataHubUpgradeState result();

  /** Returns the action to perform after executing the step, either continue or abort. */
  default Action action() {
    return Action.CONTINUE;
  }
}
