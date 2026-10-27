package com.linkedin.metadata.usage.flush;

import io.datahubproject.metadata.context.usage.AttributionType;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nonnull;

/**
 * Mutable in-memory set of unique catalog identities for one {@code (identity_metric, actor_class)}
 * rollup bucket. Keys on {@code usageIdentity} only; attribution is retained for downstream sinks
 * (downstream export, audit) but does not create duplicate identities.
 */
public final class DistinctIdentitySet {

  private final ConcurrentHashMap<String, AttributionType> identities = new ConcurrentHashMap<>();

  /** Records the identity, returning {@code true} when this is a new unique identity. */
  public boolean add(@Nonnull String usageIdentity, @Nonnull AttributionType attribution) {
    return identities.putIfAbsent(usageIdentity, attribution) == null;
  }

  public boolean isEmpty() {
    return identities.isEmpty();
  }

  public int size() {
    return identities.size();
  }

  @Nonnull
  public List<DistinctIdentityEntry> toEntries() {
    List<DistinctIdentityEntry> entries = new ArrayList<>(identities.size());
    for (var entry : identities.entrySet()) {
      entries.add(new DistinctIdentityEntry(entry.getKey(), entry.getValue()));
    }
    return List.copyOf(entries);
  }

  public void mergeFrom(@Nonnull Iterable<DistinctIdentityEntry> entries) {
    for (DistinctIdentityEntry entry : entries) {
      add(entry.usageIdentity(), entry.attributionType());
    }
  }
}
