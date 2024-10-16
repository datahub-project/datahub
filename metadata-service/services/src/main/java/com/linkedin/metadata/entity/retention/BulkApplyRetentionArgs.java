package com.linkedin.metadata.entity.retention;

import lombok.Data;

@Data
public class BulkApplyRetentionArgs {
  public Integer start;
  public Integer count;
  public Integer attemptWithVersion;
  public String aspectName;
  public String urn;
}
