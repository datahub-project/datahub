package com.linkedin.metadata.usage.store;

import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RequestContext;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * In-memory store for GMS API usage aggregation (operational Micrometer + optional flush sinks).
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

  /**
   * Report-driven usage from trusted reporters (not HTTP request-path {@link #recordRequest}).
   *
   * <p>Pass the system {@link OperationContext} for actor-class aspect reads and an attributed
   * {@link RequestContext} built by the reporter. Does not invoke {@code SessionContextEnricher} /
   * request-path {@code api_calls}. Increments only metrics with {@code emit_when: reported}.
   * Default no-op when a deployment does not implement the hook.
   *
   * @return true when at least one report-driven metric was recorded
   */
  default boolean recordReportedUsage(
      @Nonnull OperationContext systemOperationContext,
      @Nonnull RequestContext requestContext,
      long quantity) {
    return false;
  }
}
