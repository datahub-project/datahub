package com.linkedin.metadata.graph.cache;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Builder;
import lombok.Value;

/** One entity-level graph cache invalidation event from a sync metadata write. */
@Value
@Builder
public class SyncGraphInvalidationEntry {
  @Nonnull String entityUrn;
  @Nonnull String entityType;

  /**
   * Relationship aspect that changed, or {@code null} for entity-wide delete.
   *
   * <p>Creates and updates always set this to the written aspect. Deletes set {@code null} when the
   * key aspect was removed (whole entity delete) so invalidation scans graphs by {@code entityType}
   * rather than aspect-indexed lookup — key aspects are not registered on graph edges.
   */
  @Nullable String aspectName;
}
