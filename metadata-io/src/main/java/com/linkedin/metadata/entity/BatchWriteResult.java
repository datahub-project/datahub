package com.linkedin.metadata.entity;

import com.linkedin.common.urn.Urn;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

/**
 * Aggregated per-MCP outcomes for one batch write on the optimistic-locking path. Replaces
 * throw-on-first-conflict: the persist loop collects an {@link AspectWriteResult} per item and the
 * caller inspects conflicts / failures as data.
 */
@Immutable
public final class BatchWriteResult {

  @Nonnull private final List<AspectWriteResult> results;

  public BatchWriteResult(@Nonnull List<AspectWriteResult> results) {
    this.results = List.copyOf(results);
  }

  @Nonnull
  public List<AspectWriteResult> getResults() {
    return results;
  }

  /** URNs with at least one CONFLICT — the input to scoped (branch-keyed) retry. */
  @Nonnull
  public Set<Urn> conflictedUrns() {
    return results.stream()
        .filter(r -> r.getOutcome() == AspectWriteOutcome.CONFLICT)
        .map(AspectWriteResult::getUrn)
        .collect(Collectors.toSet());
  }

  @Nonnull
  public List<AspectWriteResult> failureResults() {
    return results.stream()
        .filter(r -> r.getOutcome() == AspectWriteOutcome.FAILED)
        .collect(Collectors.toList());
  }

  @Nonnull
  public List<AspectWriteResult> committedResults() {
    return results.stream()
        .filter(r -> r.getOutcome() == AspectWriteOutcome.COMMITTED)
        .collect(Collectors.toList());
  }

  public boolean hasConflicts() {
    return results.stream().anyMatch(r -> r.getOutcome() == AspectWriteOutcome.CONFLICT);
  }
}
