package com.linkedin.datahub.upgrade;

import java.util.List;


public interface UpgradeReport {

  void addLine(String line);

  List<String> lines();

}
