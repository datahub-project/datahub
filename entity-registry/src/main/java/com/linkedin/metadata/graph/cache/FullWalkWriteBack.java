package com.linkedin.metadata.graph.cache;

import java.util.List;
import java.util.Set;
import javax.annotation.Nonnull;
import lombok.Builder;
import lombok.Value;

/**
 * A walk the caller already finished. The cache publishes it only when {@link #truncated} is false.
 */
@Value
@Builder
public class FullWalkWriteBack {
  @Nonnull String graphId;
  @Nonnull GraphSnapshotSource source;
  @Nonnull TraversalDirection direction;
  @Nonnull Set<String> seeds;
  @Nonnull List<FullWalkEdge> edges;
  long invalidationGenerationAtStart;

  /**
   * Epoch millis at which the walk began reading its source. The cache stamps the component with
   * it, so a walk that read the index before a write became visible does not look newer than that
   * write. Zero means unknown, and the publish time is used.
   */
  long walkStartedAtMillis;

  boolean truncated;
}
