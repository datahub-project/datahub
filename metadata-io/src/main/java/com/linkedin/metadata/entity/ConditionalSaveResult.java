package com.linkedin.metadata.entity;

import com.linkedin.metadata.aspect.EntityAspect;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

/**
 * Result of {@link AspectDao#saveLatestAspectConditional}. Carries the outcome plus the rows written
 * so an empty update is never ambiguous between a no-op and a conflict.
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

  /** The version-N history row written in the same transaction, if retention kept one. */
  @Nonnull
  public Optional<EntityAspect> getInserted() {
    return inserted;
  }

  /** The updated version-0 row. Present only when {@link #getOutcome()} is UPDATED. */
  @Nonnull
  public Optional<EntityAspect> getUpdated() {
    return updated;
  }
}
