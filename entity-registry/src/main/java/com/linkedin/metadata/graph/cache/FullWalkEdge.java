package com.linkedin.metadata.graph.cache;

import javax.annotation.Nonnull;
import lombok.Builder;
import lombok.Value;

/** Stored-orientation edge from a walk the caller already performed. */
@Value
@Builder
public class FullWalkEdge {
  @Nonnull String sourceUrn;
  @Nonnull String destinationUrn;
  @Nonnull String relationshipType;
}
