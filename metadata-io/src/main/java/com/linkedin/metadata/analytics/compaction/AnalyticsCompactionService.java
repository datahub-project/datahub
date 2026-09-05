package com.linkedin.metadata.analytics.compaction;

import javax.annotation.Nonnull;

/**
 * Progressive analytics compaction (hour → day → month). Implementations are store-specific;
 * pgAnalytics is the only backend today.
 */
public interface AnalyticsCompactionService {

  @Nonnull
  AnalyticsCompactionResult compact(@Nonnull AnalyticsCompactionRequest request);

  /** Stable identifier for metrics/ops (e.g. {@code pgAnalytics}). */
  @Nonnull
  String implementation();
}
