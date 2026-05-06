package com.linkedin.metadata.timeseries;

import com.hazelcast.map.EntryProcessor;
import java.io.Serializable;
import java.util.Map;

/**
 * Hazelcast entry processor that atomically replaces a {@link CachedLatestAspect} only when the
 * incoming wrapper carries a {@code timestampMillis} greater than or equal to the existing one.
 *
 * <p>This runs server-side on the Hazelcast member that owns the partition for the cache key, so
 * the read-modify-write is atomic across the cluster — preventing concurrent GMS replicas from
 * racing each other and clobbering newer cached values with older ones.
 *
 * <p>The map value type is {@link CachedLatestAspect} — the data cache is a separate IMap from the
 * reverse-index cache, so we get the strong typing for free.
 *
 * <p>TTL is intentionally not set here; entry processors don't carry a TTL contract. Callers apply
 * {@code IMap.setTtl(...)} after a successful write.
 */
public class PutIfNewerProcessor
    implements EntryProcessor<String, CachedLatestAspect, Boolean>, Serializable {
  private static final long serialVersionUID = 1L;

  private final CachedLatestAspect incoming;

  public PutIfNewerProcessor(CachedLatestAspect incoming) {
    this.incoming = incoming;
  }

  @Override
  public Boolean process(Map.Entry<String, CachedLatestAspect> entry) {
    CachedLatestAspect existing = entry.getValue();

    // Empty slot — accept unconditionally.
    if (existing == null) {
      entry.setValue(incoming);
      return true;
    }

    if (incoming.getTimestampMillis() >= existing.getTimestampMillis()) {
      entry.setValue(incoming);
      return true;
    }

    return false;
  }
}
