package com.linkedin.metadata.cache.visibility;

import javax.annotation.Nonnull;

/** Flushes the pending bulk writes of one index kind. */
@FunctionalInterface
public interface IndexWriteFlusher {

  /**
   * A flusher that never flushes. {@link VisibilityMode#FLUSH_AWAIT} then uses the submit bound.
   */
  IndexWriteFlusher NONE = kind -> false;

  /**
   * Blocks until every write pending for {@code kind} has been acknowledged by the search cluster.
   *
   * @return true when all pending writes were acknowledged without failures. False when flushing is
   *     unsupported or any write failed, in which case callers fall back to the submit bound.
   */
  boolean flushAndAwait(@Nonnull SourceIndexKind kind);
}
