package com.linkedin.metadata.usage.flush;

import javax.annotation.Nonnull;

/** Extension point — additional flush sinks register as Spring beans. */
public interface UsageFlushSink {

  void publish(@Nonnull UsageFlushBatch batch);
}
