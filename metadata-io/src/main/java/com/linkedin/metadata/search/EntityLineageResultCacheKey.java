package com.linkedin.metadata.search;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.graph.LineageDirection;
import javax.annotation.Nonnull;
import lombok.Data;

@Data
public class EntityLineageResultCacheKey {
  private final String contextId;
  private final Urn sourceUrn;
  private final LineageDirection direction;
  private final Integer maxHops;
  private final Integer entitiesExploredPerHopLimit;

  public EntityLineageResultCacheKey(
      @Nonnull String contextId,
      Urn sourceUrn,
      LineageDirection direction,
      Integer maxHops,
      Integer entitiesExploredPerHopLimit) {
    this.contextId = contextId;
    this.sourceUrn = sourceUrn;
    this.direction = direction;
    this.maxHops = maxHops;
    this.entitiesExploredPerHopLimit = entitiesExploredPerHopLimit;
  }
}
