package com.linkedin.metadata.entity.retention;

import io.datahubproject.metadata.context.OperationContext;
import lombok.Data;

@Data
public class BulkApplyRetentionArgs {
  public OperationContext opContext;
  public Integer start;
  public Integer count;
  public Integer attemptWithVersion;
  public String aspectName;
  public String urn;
}
