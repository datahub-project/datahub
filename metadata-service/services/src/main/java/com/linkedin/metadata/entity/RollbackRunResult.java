package com.linkedin.metadata.entity;

import com.linkedin.metadata.run.AspectRowSummary;
import java.util.List;
import javax.annotation.Nullable;
import lombok.AllArgsConstructor;
import lombok.Value;

@Value
@AllArgsConstructor
public class RollbackRunResult {
  public List<AspectRowSummary> rowsRolledBack;
  public Integer rowsDeletedFromEntityDeletion;
  public List<RollbackResult> rollbackResults;

  /**
   * Set (never null) only by {@code EntityService#deleteUrn(OperationContext, Urn, DeleteCeiling)};
   * null for every other operation.
   */
  @Nullable public ConditionalDeleteOutcome conditionalDeleteOutcome;

  public RollbackRunResult(
      List<AspectRowSummary> rowsRolledBack,
      Integer rowsDeletedFromEntityDeletion,
      List<RollbackResult> rollbackResults) {
    this(rowsRolledBack, rowsDeletedFromEntityDeletion, rollbackResults, null);
  }
}
