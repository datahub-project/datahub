package com.linkedin.metadata.systemmetadata.metrics;

import com.linkedin.metadata.systemmetadata.KeyAspectEntityCountResult;
import com.linkedin.metadata.systemmetadata.PlatformEntityCountResult;
import javax.annotation.Nonnull;

/** Extension point — additional entity count sinks register as Spring beans. */
public interface EntityCountMetricsSink {

  void publish(@Nonnull KeyAspectEntityCountResult result);

  /**
   * Optional platform×entity_type inventory land. Default no-op so Micrometer and other type-only
   * sinks stay unchanged.
   */
  default void publishPlatform(@Nonnull PlatformEntityCountResult result) {}
}
