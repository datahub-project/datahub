package com.linkedin.metadata.config.search;

import lombok.Data;

@Data
public class GraphQueryConfiguration {

  private long timeoutSeconds;
  private int batchSize;
  private int maxResult;
  // When set to true, the graph walk (typically in search-across-lineage or scroll-across-lineage)
  // will return all paths between the source and destination nodes within the hops limit.
  private boolean enableMultiPathSearch;

  public static GraphQueryConfiguration testDefaults;

  static {
    testDefaults = new GraphQueryConfiguration();
    testDefaults.setBatchSize(1000);
    testDefaults.setTimeoutSeconds(10);
    testDefaults.setMaxResult(10000);
    testDefaults.setEnableMultiPathSearch(true);
  }
}
