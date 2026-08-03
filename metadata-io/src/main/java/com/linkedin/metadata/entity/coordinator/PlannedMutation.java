package com.linkedin.metadata.entity.coordinator;

import javax.annotation.Nonnull;

/**
 * A single proposed change to one aspect row, identified by its {@link AspectKey}. Implementations
 * are intentionally thin wrappers around the existing write-path types ({@code ChangeMCP}) so that
 * downstream apply can reuse the existing entity-service write logic unchanged.
 */
public sealed interface PlannedMutation permits PlannedUpsert, PlannedDelete {

  /** The aspect row this mutation targets. */
  @Nonnull
  AspectKey key();
}
