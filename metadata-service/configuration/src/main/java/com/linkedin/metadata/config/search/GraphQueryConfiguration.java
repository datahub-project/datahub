package com.linkedin.metadata.config.search;

import lombok.Data;

@Data
public class GraphQueryConfiguration {

  private long timeoutSeconds;
  private int batchSize;
  private int maxResult;

  public static GraphQueryConfiguration testDefaults;

  static {
    testDefaults = new GraphQueryConfiguration();
    testDefaults.setBatchSize(1000);
    testDefaults.setTimeoutSeconds(10);
    testDefaults.setMaxResult(10000);
  }
}
