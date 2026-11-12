package com.linkedin.metadata.messaging;

import javax.annotation.Nonnull;

/** Transport-neutral consumer lag / offset snapshot for MCP and MCL topics. */
public interface ConsumerLagPort {

  @Nonnull
  String transport();

  @Nonnull
  ConsumerGroupLagSnapshot mcpLag(boolean skipCache, boolean detailed);

  @Nonnull
  ConsumerGroupLagSnapshot mclVersionedLag(boolean skipCache, boolean detailed);

  @Nonnull
  ConsumerGroupLagSnapshot mclTimeseriesLag(boolean skipCache, boolean detailed);

  /** Lag for the DataHub usage-event stream (audit / analytics indexing). */
  @Nonnull
  ConsumerGroupLagSnapshot usageEventsLag(boolean skipCache, boolean detailed);
}
