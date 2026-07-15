package com.linkedin.metadata.kafka.usage;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.linkedin.metadata.datahubusage.postgres.PostgresUsageEventInsertRow;
import com.linkedin.metadata.datahubusage.postgres.PostgresUsageEventsJson;
import com.linkedin.metadata.datahubusage.postgres.PostgresUsageEventsStore;
import io.datahubproject.metadata.context.OperationContext;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;

@Slf4j
public class PostgresDataHubUsageEventIndexer implements DataHubUsageEventIndexer {

  private final PostgresUsageEventsStore store;
  private final OperationContext systemOperationContext;

  public PostgresDataHubUsageEventIndexer(
      PostgresUsageEventsStore store,
      @Qualifier("systemOperationContext") OperationContext systemOperationContext) {
    this.store = store;
    this.systemOperationContext = systemOperationContext;
  }

  /**
   * Convert each event in the Kafka batch into a {@link PostgresUsageEventInsertRow} and insert
   * them via {@link PostgresUsageEventsStore#insertBatch(List)}, which executes a single JDBC
   * transaction (autoCommit=false + executeBatch + commit).
   *
   * <p>Events whose JSON cannot be parsed are skipped with a warning so a single bad record does
   * not poison the whole batch; structural insert failures are propagated so the Kafka listener can
   * back off and let the consumer redeliver.
   */
  @Override
  public void indexBatch(
      @Nonnull OperationContext opContext, @Nonnull List<IndexableUsageEvent> events) {
    if (events.isEmpty()) {
      return;
    }
    List<PostgresUsageEventInsertRow> rows = new ArrayList<>(events.size());
    for (IndexableUsageEvent event : events) {
      try {
        rows.add(
            PostgresUsageEventsJson.parse(
                opContext,
                event.documentIdWithKafkaOffsetSuffix(),
                event.document().getDocument()));
      } catch (JsonProcessingException e) {
        log.warn(
            "Skipping usage event: invalid JSON for id {}",
            event.documentIdWithKafkaOffsetSuffix(),
            e);
      }
    }
    if (rows.isEmpty()) {
      return;
    }
    try {
      store.insertBatch(rows);
    } catch (SQLException e) {
      log.error("PostgreSQL usage-event batch insert failed (size={})", rows.size(), e);
      throw new RuntimeException("PostgreSQL usage event insert failed", e);
    }
  }
}
