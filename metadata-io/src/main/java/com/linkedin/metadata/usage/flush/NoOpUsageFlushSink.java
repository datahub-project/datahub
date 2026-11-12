package com.linkedin.metadata.usage.flush;

import io.datahubproject.metadata.context.OperationContext;
import javax.annotation.Nonnull;

public class NoOpUsageFlushSink implements UsageFlushSink {
  @Override
  public void publish(@Nonnull OperationContext opContext, @Nonnull UsageFlushBatch batch) {}
}
