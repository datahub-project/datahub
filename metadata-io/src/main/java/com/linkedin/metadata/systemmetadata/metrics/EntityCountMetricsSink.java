package com.linkedin.metadata.systemmetadata.metrics;

import com.linkedin.metadata.systemmetadata.KeyAspectEntityCountResult;
import javax.annotation.Nonnull;

/** Extension point — additional entity count sinks register as Spring beans. */
public interface EntityCountMetricsSink {

  void publish(@Nonnull KeyAspectEntityCountResult result);
}
