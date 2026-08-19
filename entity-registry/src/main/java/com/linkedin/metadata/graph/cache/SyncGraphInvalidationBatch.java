package com.linkedin.metadata.graph.cache;

import java.util.Set;
import javax.annotation.Nonnull;
import lombok.Builder;
import lombok.Singular;
import lombok.Value;

/**
 * Batch of sync-write graph cache invalidation events.
 *
 * <p>Processed by {@code EntityGraphCacheService.invalidateOnSyncBatch} in order: creates, deletes,
 * updates. Empty batches are no-ops.
 */
@Value
@Builder
public class SyncGraphInvalidationBatch {

  private static final SyncGraphInvalidationBatch EMPTY =
      SyncGraphInvalidationBatch.builder().build();

  @Singular("create")
  @Nonnull
  Set<SyncGraphInvalidationEntry> creates;

  @Singular("delete")
  @Nonnull
  Set<SyncGraphInvalidationEntry> deletes;

  @Singular("update")
  @Nonnull
  Set<SyncGraphInvalidationEntry> updates;

  @Nonnull
  public static SyncGraphInvalidationBatch empty() {
    return EMPTY;
  }

  public boolean isEmpty() {
    return creates.isEmpty() && deletes.isEmpty() && updates.isEmpty();
  }
}
