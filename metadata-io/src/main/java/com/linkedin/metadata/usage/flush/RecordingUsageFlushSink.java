package com.linkedin.metadata.usage.flush;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nonnull;

/** Test sink that records flush batches for assertions. */
public class RecordingUsageFlushSink implements UsageFlushSink {

  private final List<UsageFlushBatch> batches = Collections.synchronizedList(new ArrayList<>());

  @Override
  public void publish(@Nonnull UsageFlushBatch batch) {
    batches.add(batch);
  }

  @Nonnull
  public List<UsageFlushBatch> batches() {
    return List.copyOf(batches);
  }

  public void clear() {
    batches.clear();
  }
}
