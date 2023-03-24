package com.linkedin.metadata.search;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.graph.LineageDirection;
import lombok.Data;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;


@Data
public class EntityLineageResultCacheKey {
  private final Urn sourceUrn;
  private final LineageDirection direction;
  private final Long startTimeMillis;
  private final Long endTimeMillis;
  private final Integer maxHops;

  // Force use of static method outside of package (for tests)
  EntityLineageResultCacheKey(Urn sourceUrn, LineageDirection direction, Long startTimeMillis, Long endTimeMillis, Integer maxHops) {
    this.sourceUrn = sourceUrn;
    this.direction = direction;
    this.startTimeMillis = startTimeMillis;
    this.endTimeMillis = endTimeMillis;
    this.maxHops = maxHops;
  }

  public static EntityLineageResultCacheKey from(Urn sourceUrn, LineageDirection direction, Long startTimeMillis,
                                                 Long endTimeMillis, Integer maxHops) {
    return EntityLineageResultCacheKey.from(sourceUrn, direction, startTimeMillis, endTimeMillis, maxHops, ChronoUnit.DAYS);
  }

  public static EntityLineageResultCacheKey from(Urn sourceUrn, LineageDirection direction, Long startTimeMillis,
                                                 Long endTimeMillis, Integer maxHops, TemporalUnit resolution) {
    long endOffset = resolution.getDuration().getSeconds() * 1000;
    return new EntityLineageResultCacheKey(sourceUrn, direction,
        startTimeMillis == null ? null
            : Instant.ofEpochMilli(startTimeMillis).truncatedTo(resolution).toEpochMilli(),
        endTimeMillis == null ? null : Instant.ofEpochMilli(endTimeMillis + endOffset).truncatedTo(resolution).toEpochMilli(),
        maxHops);
  }
}
