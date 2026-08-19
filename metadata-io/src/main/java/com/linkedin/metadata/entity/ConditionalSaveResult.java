package com.linkedin.metadata.entity;

import com.linkedin.metadata.aspect.EntityAspect;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

/**
 * Result of {@link AspectDao#saveLatestAspectConditional}. Distinguishes UPDATED / SKIPPED_NOOP /
 * CONFLICT so an empty update is not ambiguous.
 */
@Immutable
public final class ConditionalSaveResult {

  @Nonnull private final ConditionalWriteOutcome outcome;
  @Nonnull private final Optional<EntityAspect> inserted;
  @Nonnull private final Optional<EntityAspect> updated;

  public ConditionalSaveResult(
      @Nonnull ConditionalWriteOutcome outcome,
      @Nonnull Optional<EntityAspect> inserted,
      @Nonnull Optional<EntityAspect> updated) {
    this.outcome = outcome;
    this.inserted = inserted;
    this.updated = updated;
  }

  @Nonnull
  public ConditionalWriteOutcome getOutcome() {
    return outcome;
  }

  /** The version-N history row inserted in the same transaction, if any. */
  @Nonnull
  public Optional<EntityAspect> getInserted() {
    return inserted;
  }

  /**
   * The updated version-0 row. Present iff {@link #getOutcome()} is {@link
   * ConditionalWriteOutcome#UPDATED}; empty for {@link ConditionalWriteOutcome#SKIPPED_NOOP} and
   * {@link ConditionalWriteOutcome#CONFLICT}.
   */
  @Nonnull
  public Optional<EntityAspect> getUpdated() {
    return updated;
  }
}
