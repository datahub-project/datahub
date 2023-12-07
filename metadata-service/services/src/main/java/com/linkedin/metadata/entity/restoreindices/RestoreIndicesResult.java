package com.linkedin.metadata.entity.restoreindices;

import lombok.Data;

@Data
public class RestoreIndicesResult {
  public int ignored = 0;
  public int rowsMigrated = 0;
  public long timeSqlQueryMs = 0;
  public long timeGetRowMs = 0;
  public long timeUrnMs = 0;
  public long timeEntityRegistryCheckMs = 0;
  public long aspectCheckMs = 0;
  public long createRecordMs = 0;
  public long sendMessageMs = 0;
}
