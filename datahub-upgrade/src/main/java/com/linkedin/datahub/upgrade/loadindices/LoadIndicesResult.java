package com.linkedin.datahub.upgrade.loadindices;

import lombok.Data;

@Data
public class LoadIndicesResult {
  public int rowsProcessed = 0;
  public int ignored = 0;
  public long timeSqlQueryMs = 0;
  public long timeElasticsearchWriteMs = 0;
  public long unknownToRegistrySkipped = 0;

  @Override
  public String toString() {
    return String.format(
        "LoadIndicesResult{rowsProcessed=%d, ignored=%d, timeSqlQueryMs=%d, timeElasticsearchWriteMs=%d, unknownToRegistrySkipped=%d}",
        rowsProcessed,
        ignored,
        timeSqlQueryMs,
        timeElasticsearchWriteMs,
        unknownToRegistrySkipped);
  }
}
