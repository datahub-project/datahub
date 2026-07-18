package com.linkedin.metadata.timeseries.write.postgres;

import com.fasterxml.jackson.databind.JsonNode;
import com.linkedin.metadata.timeseries.postgres.PostgresTimeseriesAspectDao;
import com.linkedin.metadata.timeseries.write.AbstractTimeseriesAspectWriteSink;
import com.linkedin.metadata.timeseries.write.AbstractTimeseriesAspectWriteSink.TimeseriesAspectRowPayload;
import io.datahubproject.metadata.context.OperationContext;
import java.sql.SQLException;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * JDBC dual-write for SqlSetup {@code {prefix}_aspect_row} (see {@code
 * datahub-upgrade/src/main/resources/sqlsetup/pgtimeseries/}). Skipped when PostgreSQL is already
 * the primary {@link com.linkedin.metadata.timeseries.TimeseriesAspectService} implementation (see
 * factory).
 */
@Slf4j
@RequiredArgsConstructor
public class PostgresTimeseriesAspectWriteSink extends AbstractTimeseriesAspectWriteSink {

  @Nonnull private final PostgresTimeseriesAspectDao pgTimeseriesAspectDao;

  @Override
  public void upsertDocument(
      @Nonnull OperationContext opContext,
      @Nonnull String entityName,
      @Nonnull String aspectName,
      @Nonnull String docId,
      @Nonnull JsonNode document) {
    TimeseriesAspectRowPayload row = parsePayload(entityName, aspectName, docId, document);
    try {
      pgTimeseriesAspectDao.upsert(row);
    } catch (SQLException e) {
      log.error(
          "Postgres timeseries dual-write failed for {} {} messageId={}: {}",
          entityName,
          aspectName,
          row.getMessageId(),
          e.getMessage(),
          e);
    }
  }

  @Override
  public void deleteDocument(
      @Nonnull OperationContext opContext,
      @Nonnull String entityName,
      @Nonnull String aspectName,
      @Nonnull String docId,
      @Nullable JsonNode document,
      @SuppressWarnings("unused") boolean isExploded) {
    // Deletes by resolved message id only (same as ES doc identity); isExploded is unused.
    String messageId = resolveMessageId(docId, document);
    try {
      pgTimeseriesAspectDao.deleteByMessageId(entityName, aspectName, messageId);
    } catch (SQLException e) {
      log.error(
          "Postgres timeseries delete failed for {} {} messageId={}: {}",
          entityName,
          aspectName,
          messageId,
          e.getMessage(),
          e);
    }
  }
}
