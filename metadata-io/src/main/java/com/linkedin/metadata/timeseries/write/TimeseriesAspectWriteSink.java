package com.linkedin.metadata.timeseries.write;

import com.fasterxml.jackson.databind.JsonNode;
import io.datahubproject.metadata.context.OperationContext;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Optional PostgreSQL store for timeseries aspect rows. Shape matches {@link
 * com.linkedin.metadata.timeseries.TimeseriesAspectService#upsertDocument} inputs — the same
 * ES-shaped JSON produced by {@link
 * com.linkedin.metadata.timeseries.transformer.TimeseriesAspectTransformer}.
 *
 * <p>Elasticsearch timeseries writes are handled separately by {@link
 * com.linkedin.metadata.timeseries.TimeseriesAspectService} in {@link
 * com.linkedin.metadata.service.UpdateIndicesV2Strategy}; this sink is for PostgreSQL when {@code
 * postgres.pgTimeseries.enabled} is true.
 */
public interface TimeseriesAspectWriteSink {

  TimeseriesAspectWriteSink NOOP =
      (opContext, entityName, aspectName, docId, document) -> {
        // no-op
      };

  void upsertDocument(
      @Nonnull OperationContext opContext,
      @Nonnull String entityName,
      @Nonnull String aspectName,
      @Nonnull String docId,
      @Nonnull JsonNode document);

  /** Deletes the row keyed like {@link #upsertDocument}. */
  default void deleteDocument(
      @Nonnull OperationContext opContext,
      @Nonnull String entityName,
      @Nonnull String aspectName,
      @Nonnull String docId,
      boolean isExploded) {
    deleteDocument(opContext, entityName, aspectName, docId, null, isExploded);
  }

  /**
   * Deletes the row keyed like {@link #upsertDocument}. Pass {@code document} when available so the
   * JDBC {@code message_id} matches upserts (logical {@code messageId} when present in JSON, else
   * {@code docId}).
   */
  default void deleteDocument(
      @Nonnull OperationContext opContext,
      @Nonnull String entityName,
      @Nonnull String aspectName,
      @Nonnull String docId,
      @Nullable JsonNode document,
      boolean isExploded) {
    // optional secondary store
  }
}
