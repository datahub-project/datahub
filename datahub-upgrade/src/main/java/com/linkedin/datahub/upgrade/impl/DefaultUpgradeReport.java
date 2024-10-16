package com.linkedin.datahub.upgrade.impl;

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
  }

  @Override
  public List<String> lines() {
    return reportLines;
  }
}
