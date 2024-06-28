package com.linkedin.metadata.entity;

import com.linkedin.metadata.run.AspectRowSummary;
import java.util.List;
import lombok.Value;

@Value
public class RollbackRunResult {
  public List<AspectRowSummary> rowsRolledBack;
  public Integer rowsDeletedFromEntityDeletion;
  public List<RollbackResult> rollbackResults;
}
