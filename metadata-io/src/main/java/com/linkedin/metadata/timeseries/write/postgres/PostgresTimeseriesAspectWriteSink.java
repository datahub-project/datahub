package com.linkedin.metadata.timeseries.write.postgres;

import com.fasterxml.jackson.databind.JsonNode;
import com.linkedin.metadata.timeseries.postgres.PgTimeseriesStoreRegistry;
import com.linkedin.metadata.timeseries.write.AbstractTimeseriesAspectWriteSink;
import com.linkedin.metadata.timeseries.write.AbstractTimeseriesAspectWriteSink.TimeseriesAspectRowPayload;
import io.datahubproject.metadata.context.OperationContext;
import java.sql.SQLException;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * JDBC dual-write for SqlSetup {@code {prefix}_aspect} (see {@code
 * datahub-upgrade/src/main/resources/sqlsetup/pgtimeseries/}). Skipped when PostgreSQL is already
 * the primary {@link com.linkedin.metadata.timeseries.TimeseriesAspectService} implementation (see
 * factory). Routes each {@code (entity, aspect)} to the configured store.
 */
@Slf4j
@RequiredArgsConstructor
public class PostgresTimeseriesAspectWriteSink extends AbstractTimeseriesAspectWriteSink {

  static final String UPSERT_FAILURE_METRIC = "dual_write_upsert_failure";
  static final String DELETE_FAILURE_METRIC = "dual_write_delete_failure";

  @Nonnull private final PgTimeseriesStoreRegistry storeRegistry;
  private final boolean failOnError;

  public PostgresTimeseriesAspectWriteSink(@Nonnull PgTimeseriesStoreRegistry storeRegistry) {
    this(storeRegistry, false);
  }

  @Override
  public boolean failOnError() {
    return failOnError;
  }

  @Override
  public void upsertDocument(
      @Nonnull OperationContext opContext,
      @Nonnull String entityName,
      @Nonnull String aspectName,
      @Nonnull String docId,
      @Nonnull JsonNode document) {
    TimeseriesAspectRowPayload row = parsePayload(entityName, aspectName, docId, document);
    try {
      storeRegistry.resolve(entityName, aspectName).getDao().upsert(row);
    } catch (SQLException e) {
      handleFailure(
          opContext,
          UPSERT_FAILURE_METRIC,
          "Postgres timeseries dual-write failed for {} {} messageId={}: {}",
          entityName,
          aspectName,
          row.getMessageId(),
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
    String messageId = resolveMessageId(docId, document);
    try {
      storeRegistry
          .resolve(entityName, aspectName)
          .getDao()
          .deleteByMessageId(entityName, aspectName, messageId);
    } catch (SQLException e) {
      handleFailure(
          opContext,
          DELETE_FAILURE_METRIC,
          "Postgres timeseries delete failed for {} {} messageId={}: {}",
          entityName,
          aspectName,
          messageId,
          e);
    }
  }

  @Override
  public void deleteByUrn(
      @Nonnull OperationContext opContext,
      @Nonnull String entityName,
      @Nonnull String aspectName,
      @Nonnull String urn) {
    try {
      storeRegistry
          .resolve(entityName, aspectName)
          .getDao()
          .deleteByUrn(entityName, aspectName, urn);
    } catch (SQLException e) {
      handleFailure(
          opContext,
          DELETE_FAILURE_METRIC,
          "Postgres timeseries deleteByUrn failed for {} {} urn={}: {}",
          entityName,
          aspectName,
          urn,
          e);
    }
  }

  private void handleFailure(
      @Nonnull OperationContext opContext,
      @Nonnull String metricName,
      @Nonnull String logTemplate,
      @Nonnull String entityName,
      @Nonnull String aspectName,
      @Nonnull String messageId,
      @Nonnull SQLException e) {
    opContext
        .getMetricUtils()
        .ifPresent(metricUtils -> metricUtils.increment(getClass(), metricName, 1));
    log.error(logTemplate, entityName, aspectName, messageId, e.getMessage(), e);
    if (failOnError) {
      throw new IllegalStateException(
          "PostgreSQL timeseries dual-write failed for "
              + entityName
              + " "
              + aspectName
              + " messageId="
              + messageId,
          e);
    }
  }
}
