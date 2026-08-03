package com.linkedin.metadata.entity.coordinator;

import javax.annotation.Nonnull;

/**
 * Supplies a {@link MutationPlan} computed from current, non-locking DB state.
 *
 * <p>Called once up front by the {@link MutationCoordinator}, and again after acquiring
 * coordination locks to detect conflict-key expansion (a re-plan whose closure reached a new
 * conflict domain). Implementations must be deterministic given the underlying DB state so that a
 * re-plan against unchanged state yields the same conflict-key set.
 */
@FunctionalInterface
public interface MutationPlanSource {

  /** Re-computes the plan from current (non-locking) state. */
  @Nonnull
  MutationPlan plan();
}
