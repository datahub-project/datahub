package com.linkedin.datahub.upgrade.impl;

import com.linkedin.datahub.upgrade.UpgradeReport;
import java.util.ArrayList;
import java.util.List;


public class DefaultUpgradeReport implements UpgradeReport {

  private final List<String> reportLines = new ArrayList<>();

  @Override
  public void addLine(String line) {
    System.out.println(line); // TODO: Change to logging.
    reportLines.add(line);
  }

  @Override
  public List<String> lines() {
    return reportLines;
  }
}
