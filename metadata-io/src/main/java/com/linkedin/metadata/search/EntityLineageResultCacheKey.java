package com.linkedin.metadata.search;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.graph.LineageDirection;
import lombok.Data;


@Data
public class EntityLineageResultCacheKey {
  private final Urn sourceUrn;
  private final LineageDirection direction;
  private final Long startTimeMillis;
  private final Long endTimeMillis;
  private final Integer maxHops;
}
