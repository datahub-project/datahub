package com.linkedin.metadata.search;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.graph.LineageDirection;
import java.time.Instant;
import java.time.temporal.TemporalUnit;
import lombok.Data;

@Data
public class EntityLineageResultCacheKey {
  private final Urn sourceUrn;
  private final LineageDirection direction;
  private final Long startTimeMillis;
  private final Long endTimeMillis;
  private final Integer maxHops;

  public EntityLineageResultCacheKey(
      Urn sourceUrn,
      LineageDirection direction,
      Long startTimeMillis,
      Long endTimeMillis,
      Integer maxHops,
      TemporalUnit resolution) {

    this.sourceUrn = sourceUrn;
    this.direction = direction;
    this.maxHops = maxHops;
    long endOffset = resolution.getDuration().getSeconds() * 1000;
    this.startTimeMillis =
        startTimeMillis == null
            ? null
            : Instant.ofEpochMilli(startTimeMillis).truncatedTo(resolution).toEpochMilli();
    this.endTimeMillis =
        endTimeMillis == null
            ? null
            : Instant.ofEpochMilli(endTimeMillis + endOffset)
                .truncatedTo(resolution)
                .toEpochMilli();
  }
}
