package com.linkedin.metadata.kafka.usage;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.linkedin.metadata.analytics.postgres.AnalyticsMetricFamilies;
import com.linkedin.metadata.analytics.postgres.PgAnalyticsStoreRegistry;
import com.linkedin.metadata.analytics.postgres.PostgresAnalyticsEventInsert;
import com.linkedin.metadata.analytics.postgres.PostgresAnalyticsEventJson;
import com.linkedin.metadata.analytics.postgres.PostgresAnalyticsStore;
import io.datahubproject.metadata.context.OperationContext;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class PostgresDataHubUsageEventIndexer implements DataHubUsageEventIndexer {

  private final PgAnalyticsStoreRegistry registry;

  public PostgresDataHubUsageEventIndexer(PgAnalyticsStoreRegistry registry) {
    this.registry = registry;
  }

  @Override
  public void indexBatch(
      @Nonnull OperationContext opContext, @Nonnull List<IndexableUsageEvent> events) {
    if (events.isEmpty()) {
      return;
    }
    List<PostgresAnalyticsEventInsert> rows = new ArrayList<>(events.size());
    for (IndexableUsageEvent event : events) {
      try {
        rows.add(
            PostgresAnalyticsEventJson.parseDatahubUsage(
                opContext,
                event.documentIdWithKafkaOffsetSuffix(),
                event.document().getDocument()));
      } catch (JsonProcessingException | RuntimeException e) {
        // Malformed JSON or timestamps must not fail the whole batch.
        log.warn(
            "Skipping usage event: parse failure for id {}",
            event.documentIdWithKafkaOffsetSuffix(),
            e);
      }
    }
    if (rows.isEmpty()) {
      return;
    }
    PostgresAnalyticsStore store =
        registry.resolve(AnalyticsMetricFamilies.DATAHUB_USAGE).getStore();
    try {
      store.insertEvents(rows);
    } catch (SQLException e) {
      log.error("PostgreSQL analytics event batch insert failed (size={})", rows.size(), e);
      throw new RuntimeException("PostgreSQL analytics event insert failed", e);
    }
  }
}
