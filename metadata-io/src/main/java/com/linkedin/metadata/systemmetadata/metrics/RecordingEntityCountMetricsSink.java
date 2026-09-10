package com.linkedin.metadata.systemmetadata.metrics;

import com.linkedin.metadata.systemmetadata.KeyAspectEntityCountResult;
import com.linkedin.metadata.systemmetadata.PlatformEntityCountResult;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nonnull;

/** Test sink that records published entity count results for assertions. */
public class RecordingEntityCountMetricsSink implements EntityCountMetricsSink {

  private final List<KeyAspectEntityCountResult> results =
      Collections.synchronizedList(new ArrayList<>());
  private final List<PlatformEntityCountResult> platformResults =
      Collections.synchronizedList(new ArrayList<>());

  @Override
  public void publish(@Nonnull KeyAspectEntityCountResult result) {
    results.add(result);
  }

  @Override
  public void publishPlatform(@Nonnull PlatformEntityCountResult result) {
    platformResults.add(result);
  }

  @Nonnull
  public List<KeyAspectEntityCountResult> results() {
    return List.copyOf(results);
  }

  @Nonnull
  public List<PlatformEntityCountResult> platformResults() {
    return List.copyOf(platformResults);
  }

  public void clear() {
    results.clear();
    platformResults.clear();
  }
}
