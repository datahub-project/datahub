package com.linkedin.metadata.systemmetadata.metrics;

import com.linkedin.metadata.systemmetadata.KeyAspectEntityCountResult;
import com.linkedin.metadata.systemmetadata.PlatformEntityCountResult;
import java.util.List;
import java.util.function.Consumer;
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
    forEachSink(sink -> sink.publish(result), "Entity count metrics sink {} failed");
  }

  @Override
  public void publishPlatform(@Nonnull PlatformEntityCountResult result) {
    forEachSink(
        sink -> sink.publishPlatform(result), "Entity count platform metrics sink {} failed");
  }

  private void forEachSink(
      @Nonnull Consumer<EntityCountMetricsSink> action, @Nonnull String failureMessage) {
    RuntimeException lastFailure = null;
    for (EntityCountMetricsSink sink : delegates) {
      if (sink == this) {
        continue;
      }
      try {
        action.accept(sink);
      } catch (RuntimeException e) {
        log.warn(failureMessage, sink.getClass().getSimpleName(), e);
        lastFailure = e;
      }
    }
    if (lastFailure != null) {
      throw lastFailure;
    }
  }
}
