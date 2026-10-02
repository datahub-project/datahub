package com.linkedin.metadata.search.elasticsearch.client.shim.impl;

import com.linkedin.metadata.search.utils.ESUtils;
import io.datahubproject.metadata.context.RequestStats;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.opensearch.client.RequestOptions;

/**
 * Request-attribution hooks shared by the search client shims. Every method is a no-op when no
 * {@link RequestStats} is in scope, which is the case unless {@code
 * telemetry.requestAttribution.enabled} is set.
 */
final class ShimTelemetry {
  private ShimTelemetry() {}

  /** Adds {@code X-Opaque-Id} carrying trace id, actor and request id when enabled. */
  @Nonnull
  static RequestOptions withOpaqueId(
      @Nonnull RequestOptions options, @Nullable RequestStats stats) {
    if (stats == null) {
      return options;
    }
    return stats
        .opaqueId()
        .map(id -> options.toBuilder().addHeader(ESUtils.OPAQUE_ID_HEADER, id).build())
        .orElse(options);
  }

  /** Accumulates one search-side round trip that started at {@code startNanos}. */
  static void recordSearch(@Nullable RequestStats stats, long startNanos) {
    if (stats != null) {
      stats.recordSearch(System.nanoTime() - startNanos);
    }
  }
}
