/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.datahub.upgrade.sqlsetup;

import lombok.Data;

@Data
public class SqlSetupResult {
  public int tablesCreated = 0;
  public int usersCreated = 0;
  public boolean cdcUserCreated = false;
  public long executionTimeMs = 0;
  public String errorMessage;
}
