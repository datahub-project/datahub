/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.datahub.upgrade.impl;

import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.upgrade.DataHubUpgradeState;

public class DefaultUpgradeStepResult implements UpgradeStepResult {

  private final String _stepId;
  private final DataHubUpgradeState _result;
  private final Action _action;

  public DefaultUpgradeStepResult(String stepId, DataHubUpgradeState result) {
    this(stepId, result, Action.CONTINUE);
  }

  public DefaultUpgradeStepResult(String stepId, DataHubUpgradeState result, Action action) {
    _stepId = stepId;
    _result = result;
    _action = action;
  }

  @Override
  public String stepId() {
    return _stepId;
  }

  @Override
  public DataHubUpgradeState result() {
    return _result;
  }

  @Override
  public Action action() {
    return _action;
  }
}
