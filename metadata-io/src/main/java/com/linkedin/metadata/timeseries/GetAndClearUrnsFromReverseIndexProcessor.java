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
 * <p>Map value type is {@link Object} because the same {@code IMap} also stores {@link
 * CachedLatestAspect} (and possibly legacy raw {@link String}) entries under different keys. Only
 * acts on {@code Set<String>}-shaped values; other shapes are treated as empty.
 */
public class GetAndClearUrnsFromReverseIndexProcessor
    implements EntryProcessor<String, Object, Set<String>>, Serializable {
  private static final long serialVersionUID = 1L;

  @Override
  @SuppressWarnings("unchecked")
  public Set<String> process(Map.Entry<String, Object> entry) {
    Object value = entry.getValue();
    if (!(value instanceof Set<?> existing) || existing.isEmpty()) {
      return Set.of();
    }

    Set<String> copy = new HashSet<>((Set<String>) existing);
    // Clears/removes this reverse-index entry atomically.
    entry.setValue(null);
    return copy;
  }
}
