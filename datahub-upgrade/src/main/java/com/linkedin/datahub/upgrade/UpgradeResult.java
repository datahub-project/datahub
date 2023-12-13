package com.linkedin.datahub.upgrade;

/** Represents the result of executing an {@link Upgrade} */
public interface UpgradeResult {

  /** The execution result. */
  enum Result {
    /** Upgrade succeeded. */
    SUCCEEDED,
    /** Upgrade failed. */
    FAILED,
    /** Upgrade was aborted. */
    ABORTED
  }

  /** Returns the {@link Result} of executing an {@link Upgrade} */
  Result result();

  /** Returns the {@link UpgradeReport} associated with the completed {@link Upgrade}. */
  UpgradeReport report();
}
