package com.linkedin.metadata.usage.flush;

import java.util.List;
import java.util.Objects;
import javax.annotation.Nonnull;

/**
 * Immutable flush payload for one distinct usage metric and {@code actor_class} bucket. {@link
 * #identities()} is the full set of unique catalog identities observed in the flush window — sinks
 * such as commercial export sinks should consume this list, not a pre-aggregated count.
 */
public record DistinctUsageSnapshot(
    String metricName, String actorClass, List<DistinctIdentityEntry> identities) {

  public DistinctUsageSnapshot {
    Objects.requireNonNull(metricName, "metricName");
    Objects.requireNonNull(actorClass, "actorClass");
    identities = List.copyOf(identities);
  }

  public int distinctCount() {
    return identities.size();
  }

  @Nonnull
  public List<String> usageIdentities() {
    return identities.stream().map(DistinctIdentityEntry::usageIdentity).toList();
  }
}
