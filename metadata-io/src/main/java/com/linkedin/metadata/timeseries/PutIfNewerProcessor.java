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
 * <p>TTL is intentionally not set here; entry processors don't carry a TTL contract. Callers apply
 * {@code IMap.setTtl(...)} after a successful write.
 */
public class PutIfNewerProcessor implements EntryProcessor<String, Object, Boolean>, Serializable {
  private static final long serialVersionUID = 1L;

  private final CachedLatestAspect incoming;

  public PutIfNewerProcessor(CachedLatestAspect incoming) {
    this.incoming = incoming;
  }

  @Override
  public Boolean process(Map.Entry<String, Object> entry) {
    Object existing = entry.getValue();

    // Empty slot — accept unconditionally.
    if (existing == null) {
      entry.setValue(incoming);
      return true;
    }

    // Backward compatibility: pre-rollout entries are raw serialized strings without an
    // associated timestamp. Treat those as definitively older — the incoming wrapper wins
    // and replaces them.
    if (existing instanceof String) {
      entry.setValue(incoming);
      return true;
    }

    CachedLatestAspect current = (CachedLatestAspect) existing;
    if (incoming.getTimestampMillis() >= current.getTimestampMillis()) {
      entry.setValue(incoming);
      return true;
    }

    return false;
  }
}
