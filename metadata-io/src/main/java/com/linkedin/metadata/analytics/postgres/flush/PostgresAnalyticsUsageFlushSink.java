package com.linkedin.metadata.analytics.postgres.flush;

import com.linkedin.metadata.analytics.postgres.AnalyticsMetricFamilies;
import com.linkedin.metadata.analytics.postgres.PgAnalyticsStoreRegistry;
import com.linkedin.metadata.analytics.postgres.PostgresAnalyticsStore;
import com.linkedin.metadata.analytics.postgres.PostgresAnalyticsUtc;
import com.linkedin.metadata.usage.flush.AdditiveUsageRow;
import com.linkedin.metadata.usage.flush.DistinctUsageSnapshot;
import com.linkedin.metadata.usage.flush.UsageFlushBatch;
import com.linkedin.metadata.usage.flush.UsageFlushSink;
import io.datahubproject.metadata.context.OperationContext;
import java.sql.SQLException;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Lands api_usage flush windows into UTC-hour rollups (additive merge + distinct identity
 * sidecars). Default off via {@code postgres.pgAnalytics.sinks.apiUsageFlushEnabled}.
 */
@Slf4j
@RequiredArgsConstructor
public class PostgresAnalyticsUsageFlushSink implements UsageFlushSink {

  @Nonnull private final PgAnalyticsStoreRegistry registry;

  @Override
  public void publish(@Nonnull OperationContext opContext, @Nonnull UsageFlushBatch batch) {
    PostgresAnalyticsStore store = registry.resolve(AnalyticsMetricFamilies.API_USAGE).getStore();
    Instant hour = PostgresAnalyticsUtc.truncateToUtcHour(batch.windowStart());
    try {
      for (AdditiveUsageRow row : batch.additiveRows()) {
        Map<String, String> dims = new HashMap<>();
        if (row.dimensions() != null) {
          dims.putAll(row.dimensions());
        }
        if (row.actorClass() != null) {
          dims.put("actor_class", row.actorClass().name());
        }
        store.mergeAdditiveRollup(
            hour,
            AnalyticsMetricFamilies.GRAIN_HOUR,
            AnalyticsMetricFamilies.API_USAGE,
            row.metricName(),
            dims,
            row.valueSum(),
            1L);
      }
      for (DistinctUsageSnapshot snap : batch.distinctSnapshots()) {
        store.upsertDistinctIdentities(
            hour,
            AnalyticsMetricFamilies.GRAIN_HOUR,
            AnalyticsMetricFamilies.API_USAGE,
            snap.metricName(),
            snap.actorClass(),
            snap.usageIdentities());
      }
    } catch (SQLException e) {
      log.error("pgAnalytics api_usage flush sink failed", e);
      throw new RuntimeException("pgAnalytics UsageFlushSink failed", e);
    }
  }
}
