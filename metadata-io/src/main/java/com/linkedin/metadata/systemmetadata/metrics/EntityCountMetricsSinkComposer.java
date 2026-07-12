package com.linkedin.metadata.systemmetadata.metrics;

import com.linkedin.metadata.systemmetadata.KeyAspectEntityCountResult;
import java.util.List;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Delegates entity count refresh results to all registered {@link EntityCountMetricsSink} beans.
 */
@Slf4j
@RequiredArgsConstructor
public class EntityCountMetricsSinkComposer implements EntityCountMetricsSink {

  private final List<EntityCountMetricsSink> delegates;

  @Override
  public void publish(@Nonnull KeyAspectEntityCountResult result) {
    RuntimeException lastFailure = null;
    for (EntityCountMetricsSink sink : delegates) {
      if (sink == this) {
        continue;
      }
      try {
        sink.publish(result);
      } catch (RuntimeException e) {
        log.warn("Entity count metrics sink {} failed", sink.getClass().getSimpleName(), e);
        lastFailure = e;
      }
    }
    if (lastFailure != null) {
      throw lastFailure;
    }
  }
}
