package com.linkedin.metadata.usage.flush;

import io.datahubproject.metadata.context.OperationContext;
import javax.annotation.Nonnull;

/** Extension point — additional flush sinks register as Spring beans. */
public interface UsageFlushSink {

  /**
   * Publish a finalized flush batch. The {@link OperationContext} is supplied by the flush driver
   * (the scheduled coordinator for background flushes, or the originating request for cardinality
   * drains) rather than held by the sink, so sinks stay off the {@code systemOperationContext} bean
   * construction graph.
   */
  void publish(@Nonnull OperationContext opContext, @Nonnull UsageFlushBatch batch);
}
