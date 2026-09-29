package com.linkedin.metadata.entity.retention;

import io.datahubproject.metadata.context.OperationContext;
import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
public class BulkApplyRetentionArgs {
  @EqualsAndHashCode.Exclude public OperationContext opContext;

  public Integer start;
  public Integer count;
  public Integer attemptWithVersion;
  public String aspectName;
  public String urn;
}
