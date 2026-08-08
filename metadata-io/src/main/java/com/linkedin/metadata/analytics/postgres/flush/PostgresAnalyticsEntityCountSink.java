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
import java.util.ArrayList;
import java.util.List;
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

  static final String METRIC_ACTIVE = "entity_count_active";
  static final String METRIC_SOFT_DELETED = "entity_count_soft_deleted";

  @Nonnull private final PgAnalyticsStoreRegistry registry;

  @Override
  public void publish(@Nonnull KeyAspectEntityCountResult result) {
    PostgresAnalyticsStore store =
        registry.resolve(AnalyticsMetricFamilies.SYSTEM_USAGE).getStore();
    Instant hour = PostgresAnalyticsUtc.truncateToUtcHour(result.getComputedAt());
    List<PostgresAnalyticsStore.LatestRollupValue> values = new ArrayList<>();
    for (KeyAspectEntityCountEntry entry : result.getCounts()) {
      Map<String, String> dims = Map.of("entity_type", entry.getEntityType());
      values.add(
          new PostgresAnalyticsStore.LatestRollupValue(
              METRIC_ACTIVE, dims, entry.getActiveCount()));
      values.add(
          new PostgresAnalyticsStore.LatestRollupValue(
              METRIC_SOFT_DELETED, dims, entry.getSoftDeletedCount()));
    }
    try {
      store.replaceLatestRollups(
          hour,
          AnalyticsMetricFamilies.GRAIN_HOUR,
          AnalyticsMetricFamilies.SYSTEM_USAGE,
          List.of(METRIC_ACTIVE, METRIC_SOFT_DELETED),
          values);
    } catch (SQLException e) {
      log.error("pgAnalytics entity-count sink failed", e);
      throw new RuntimeException("pgAnalytics EntityCountMetricsSink failed", e);
    }
  }
}
