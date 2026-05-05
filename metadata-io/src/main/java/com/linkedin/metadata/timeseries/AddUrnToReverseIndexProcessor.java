package com.linkedin.metadata.timeseries;

import com.hazelcast.map.EntryProcessor;
import java.io.Serializable;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.jspecify.annotations.Nullable;

/**
 * Adds a URN to the reverse index entry stored at {@code aspect-index:<entity>:<aspect>}.
 *
 * <p>The map's value type is {@link Object} because the same Hazelcast {@code IMap} stores both
 * data entries ({@link CachedLatestAspect}) and reverse-index entries ({@code Set<String>}). Older
 * deployments may also have raw {@link String} entries left over from before the wrapper rollout.
 * The processor only acts on {@code Set<String>}-shaped values; anything else is treated as an
 * empty index and replaced.
 */
public class AddUrnToReverseIndexProcessor
    implements EntryProcessor<String, Object, Boolean>, Serializable {
  private static final long serialVersionUID = 1L;

  private final String urn;

  public AddUrnToReverseIndexProcessor(String urn) {
    this.urn = urn;
  }

  @Override
  @SuppressWarnings("unchecked")
  public Boolean process(Map.Entry<String, Object> entry) {
    Object value = entry.getValue();
    Set<String> urns;
    if (value instanceof Set<?> existing) {
      urns = (Set<String>) existing;
    } else {
      // No existing entry, or a stray non-Set value (shouldn't happen at this key, but be
      // defensive — keys are namespaced "aspect-index:" vs. "latest:" so collisions are
      // unexpected).
      urns = new HashSet<>();
    }

    boolean added = urns.add(urn);
    entry.setValue(urns);
    return added;
  }

  @Override
  public @Nullable EntryProcessor<String, Object, Boolean> getBackupProcessor() {
    return EntryProcessor.super.getBackupProcessor();
  }
}
