package com.linkedin.metadata.entity.coordinator;

import java.util.Collections;
import java.util.SortedSet;
import java.util.TreeSet;
import javax.annotation.Nonnull;

/**
 * Immutable coordination plan for a single coordinated command: the set of conflict keys the
 * command contends on.
 *
 * @param conflictKeys coordination tokens this plan contends on (see {@link ConflictKey}); sorted
 *     for deterministic lock-acquisition order in {@link MutationCoordinator}
 */
public record MutationPlan(@Nonnull SortedSet<ConflictKey> conflictKeys) {

  public MutationPlan {
    SortedSet<ConflictKey> copiedConflictKeys = new TreeSet<>();
    if (conflictKeys != null) {
      copiedConflictKeys.addAll(conflictKeys);
    }
    conflictKeys = Collections.unmodifiableSortedSet(copiedConflictKeys);
  }
}
