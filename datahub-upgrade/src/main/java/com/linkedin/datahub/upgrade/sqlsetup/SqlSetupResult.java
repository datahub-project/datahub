package com.linkedin.datahub.upgrade.sqlsetup;

import lombok.Data;

@Data
public class SqlSetupResult {
  public int tablesCreated = 0;
  public int usersCreated = 0;
  public boolean cdcUserCreated = false;
  public long executionTimeMs = 0;
  public String errorMessage;

  @Override
  public String toString() {
    return String.format(
        "SqlSetupResult{tablesCreated=%d, usersCreated=%d, cdcUserCreated=%s, executionTimeMs=%d}",
        tablesCreated, usersCreated, cdcUserCreated, executionTimeMs);
  }
}
