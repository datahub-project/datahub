/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.datahub.upgrade;

import java.util.List;

/** A human-readable record of upgrade progress + status. */
public interface UpgradeReport {

  /** Adds a new line to the upgrade report. */
  void addLine(String line);

  /** Adds a new line to the upgrade report with exception */
  void addLine(String line, Exception e);

  /** Retrieves the lines in the report. */
  List<String> lines();
}
