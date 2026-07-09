package com.linkedin.metadata.usage.flush;

import javax.annotation.Nonnull;

public class NoOpUsageFlushSink implements UsageFlushSink {
  @Override
  public void publish(@Nonnull UsageFlushBatch batch) {}
}
