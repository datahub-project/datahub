package com.linkedin.metadata.usage.flush;

import java.time.Instant;
import java.util.List;

public record UsageFlushBatch(
    Instant windowStart,
    Instant windowEnd,
    FlushTrigger trigger,
    List<AdditiveUsageRow> additiveRows,
    List<DistinctUsageSnapshot> distinctSnapshots) {

  public UsageFlushBatch {
    additiveRows = List.copyOf(additiveRows);
    distinctSnapshots = List.copyOf(distinctSnapshots);
  }

  public int totalCardinality() {
    return additiveRows.size()
        + distinctSnapshots.stream().mapToInt(s -> s.identities().size()).sum();
  }
}
