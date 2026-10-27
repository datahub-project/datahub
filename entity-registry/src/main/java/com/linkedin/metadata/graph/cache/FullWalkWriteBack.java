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
  boolean truncated;
}
