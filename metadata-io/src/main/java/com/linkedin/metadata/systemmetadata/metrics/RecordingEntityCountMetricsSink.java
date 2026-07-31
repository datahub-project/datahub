package com.linkedin.metadata.systemmetadata.metrics;

import com.linkedin.metadata.systemmetadata.KeyAspectEntityCountResult;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nonnull;

/** Test sink that records published entity count results for assertions. */
public class RecordingEntityCountMetricsSink implements EntityCountMetricsSink {

  private final List<KeyAspectEntityCountResult> results =
      Collections.synchronizedList(new ArrayList<>());

  @Override
  public void publish(@Nonnull KeyAspectEntityCountResult result) {
    results.add(result);
  }

  @Nonnull
  public List<KeyAspectEntityCountResult> results() {
    return List.copyOf(results);
  }

  public void clear() {
    results.clear();
  }
}
