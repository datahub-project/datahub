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
 * <p>Lives in the index-cache IMap (separate from the data IMap), so the value type is {@code
 * Set<String>} natively — no Object casts.
 */
public class AddUrnToReverseIndexProcessor
    implements EntryProcessor<String, Set<String>, Boolean>, Serializable {
  private static final long serialVersionUID = 1L;

  private final String urn;

  public AddUrnToReverseIndexProcessor(String urn) {
    this.urn = urn;
  }

  @Override
  public Boolean process(Map.Entry<String, Set<String>> entry) {
    Set<String> urns = entry.getValue();
    if (urns == null) {
      urns = new HashSet<>();
    }
    boolean added = urns.add(urn);
    entry.setValue(urns);
    return added;
  }

  @Override
  public @Nullable EntryProcessor<String, Set<String>, Boolean> getBackupProcessor() {
    return EntryProcessor.super.getBackupProcessor();
  }
}
