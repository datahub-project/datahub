package com.linkedin.metadata.timeseries;

import com.hazelcast.map.EntryProcessor;
import java.io.Serializable;
import java.util.Map;
import java.util.Set;
import org.jspecify.annotations.Nullable;

public class RemoveUrnFromReverseIndexProcessor
    implements EntryProcessor<String, Set<String>, Boolean>, Serializable {

  private final String urn;

  public RemoveUrnFromReverseIndexProcessor(String urn) {
    this.urn = urn;
  }

  @Override
  public Boolean process(Map.Entry<String, Set<String>> entry) {
    Set<String> urns = entry.getValue();

    if (urns == null || urns.isEmpty()) {
      return false;
    }

    boolean removed = urns.remove(urn);

    if (urns.isEmpty()) {
      entry.setValue(null);
    } else {
      entry.setValue(urns);
    }

    return removed;
  }

  @Override
  public @Nullable EntryProcessor<String, Set<String>, Boolean> getBackupProcessor() {
    return EntryProcessor.super.getBackupProcessor();
  }
}
