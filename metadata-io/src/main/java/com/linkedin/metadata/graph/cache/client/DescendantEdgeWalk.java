package com.linkedin.metadata.graph.cache.client;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.graph.cache.FullWalkEdge;
import java.util.List;
import java.util.Set;
import javax.annotation.Nonnull;
import lombok.Value;

/** Descendants and the stored-orientation edges the same scroll already returned. */
@Value
public class DescendantEdgeWalk {
  @Nonnull Set<Urn> descendants;
  @Nonnull List<FullWalkEdge> edges;
  boolean truncated;
}
