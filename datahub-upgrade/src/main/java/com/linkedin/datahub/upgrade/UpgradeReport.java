package com.linkedin.datahub.upgrade;

import java.util.List;


/**
 * A human-readable record of upgrade progress + status.
 */
public interface UpgradeReport {

  /**
   * Adds a new line to the upgrade report.
   */
  void addLine(String line);

  /**
   * Retrieves the lines in the report.
   */
  List<String> lines();

}
