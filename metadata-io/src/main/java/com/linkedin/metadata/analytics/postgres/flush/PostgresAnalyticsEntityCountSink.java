package com.linkedin.metadata.analytics.postgres.flush;

import com.linkedin.metadata.analytics.postgres.AnalyticsMetricFamilies;
import com.linkedin.metadata.analytics.postgres.PgAnalyticsStoreRegistry;
import com.linkedin.metadata.analytics.postgres.PostgresAnalyticsStore;
import com.linkedin.metadata.analytics.postgres.PostgresAnalyticsUtc;
import com.linkedin.metadata.systemmetadata.KeyAspectEntityCountEntry;
import com.linkedin.metadata.systemmetadata.KeyAspectEntityCountResult;
import com.linkedin.metadata.systemmetadata.metrics.EntityCountMetricsSink;
import java.sql.SQLException;
import java.time.Instant;
import java.util.Map;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Lands inventory gauges as system_usage latest hourly rollups. Default off via {@code
 * postgres.pgAnalytics.sinks.entityCountEnabled}.
 */
@Slf4j
@RequiredArgsConstructor
public class PostgresAnalyticsEntityCountSink implements EntityCountMetricsSink {

  @Nonnull private final PgAnalyticsStoreRegistry registry;

  @Override
  public void publish(@Nonnull KeyAspectEntityCountResult result) {
    PostgresAnalyticsStore store =
        registry.resolve(AnalyticsMetricFamilies.SYSTEM_USAGE).getStore();
    Instant hour = PostgresAnalyticsUtc.truncateToUtcHour(result.getComputedAt());
    try {
      for (KeyAspectEntityCountEntry entry : result.getCounts()) {
        Map<String, String> dims = Map.of("entity_type", entry.getEntityType());
        store.upsertLatestRollup(
            hour,
            AnalyticsMetricFamilies.GRAIN_HOUR,
            AnalyticsMetricFamilies.SYSTEM_USAGE,
            "entity_count_active",
            dims,
            entry.getActiveCount());
        store.upsertLatestRollup(
            hour,
            AnalyticsMetricFamilies.GRAIN_HOUR,
            AnalyticsMetricFamilies.SYSTEM_USAGE,
            "entity_count_soft_deleted",
            dims,
            entry.getSoftDeletedCount());
      }
    } catch (SQLException e) {
      log.error("pgAnalytics entity-count sink failed", e);
      throw new RuntimeException("pgAnalytics EntityCountMetricsSink failed", e);
    }
  }
}
