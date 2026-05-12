package com.linkedin.metadata.timeseries;

import com.hazelcast.map.EntryProcessor;
import java.io.Serializable;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Atomically reads and clears the URN set stored at the reverse-index key {@code
 * aspect-index:<entity>:<aspect>}.
 *
 * <p>Lives in the index-cache IMap (separate from the data IMap), so the value type is {@code
 * Set<String>} natively — no Object casts.
 */
public class GetAndClearUrnsFromReverseIndexProcessor
    implements EntryProcessor<String, Set<String>, Set<String>>, Serializable {
  private static final long serialVersionUID = 1L;

  @Override
  public Set<String> process(Map.Entry<String, Set<String>> entry) {
    Set<String> existing = entry.getValue();
    if (existing == null || existing.isEmpty()) {
      return Set.of();
    }

    Set<String> copy = new HashSet<>(existing);
    // Clears/removes this reverse-index entry atomically.
    entry.setValue(null);
    return copy;
  }
}
