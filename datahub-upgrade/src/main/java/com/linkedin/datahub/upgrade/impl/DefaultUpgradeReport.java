/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.datahub.upgrade.impl;

import com.google.common.base.Throwables;
import com.linkedin.datahub.upgrade.UpgradeReport;
import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DefaultUpgradeReport implements UpgradeReport {

  private final List<String> reportLines = new ArrayList<>();

  @Override
  public void addLine(String line) {
    log.info(line);
    reportLines.add(line);
  }

  @Override
  public void addLine(String line, Exception e) {
    log.error(line, e);
    reportLines.add(line + String.format(": %s", e));
    reportLines.add(
        String.format("Exception stack trace: %s", Throwables.getStackTraceAsString(e)));
  }

  @Override
  public List<String> lines() {
    return reportLines;
  }
}
