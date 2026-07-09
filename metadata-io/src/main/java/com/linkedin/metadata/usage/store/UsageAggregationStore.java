package com.linkedin.metadata.usage.store;

import io.datahubproject.metadata.context.OperationContext;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * In-memory store for GMS API usage aggregation (operational Micrometer + optional commercial flush
 * sinks).
 *
 * <p>Not to be confused with {@link com.linkedin.metadata.billing.rollup.UsageRollupStore}, the
 * legacy product-usage rollup for integrations events (MCP, ingest proposals, etc.).
 *
 * @see InMemoryUsageAggregationStore
 */
public interface UsageAggregationStore {

  /**
   * Records request-phase usage metrics for the session.
   *
   * @return true when metrics were recorded; false when the request was skipped (e.g. validation)
   */
  boolean recordRequest(@Nonnull OperationContext opContext);

  void recordResponse(@Nonnull OperationContext opContext, @Nullable Long outputBytes);

  void flush(@Nonnull com.linkedin.metadata.usage.flush.FlushTrigger trigger);
}
