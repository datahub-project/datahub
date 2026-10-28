package com.linkedin.metadata.entity.retention;

import com.linkedin.metadata.entity.RetentionService;
import java.util.Objects;
import javax.annotation.Nonnull;

/**
 * A single (key, context) pair for a batched retention apply. Bundles the {@link RetentionKey} and
 * its {@link RetentionService.RetentionContext} so the same-size, same-index invariant between the
 * two is structurally guaranteed — a single {@code List<RetentionBatchEntry>} cannot be misaligned
 * the way parallel {@code List<RetentionKey>} / {@code List<RetentionContext>} lists can.
 */
public record RetentionBatchEntry(
    @Nonnull RetentionKey key, @Nonnull RetentionService.RetentionContext context) {
  public RetentionBatchEntry {
    Objects.requireNonNull(key, "key");
    Objects.requireNonNull(context, "context");
  }
}
