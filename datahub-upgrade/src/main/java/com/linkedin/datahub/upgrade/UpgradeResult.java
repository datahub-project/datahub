package com.linkedin.datahub.upgrade;

public interface UpgradeResult {

  enum Result {
    SUCCEEDED,
    FAILED,
    ABORTED
  }

  Result result();

  UpgradeReport report();

}
