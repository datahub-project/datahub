package com.linkedin.metadata.search.write;

import com.linkedin.common.urn.Urn;
import io.datahubproject.metadata.context.OperationContext;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Optional secondary index for entity search documents (e.g. PostgreSQL pgSearch). Shapes mirror
 * {@link com.linkedin.metadata.search.elasticsearch.ElasticSearchService} upsert/delete by search
 * group used by V3 indexing.
 */
public interface EntitySearchWriteSink {

  EntitySearchWriteSink NOOP =
      new EntitySearchWriteSink() {
        @Override
        public void upsertDocumentBySearchGroup(
            @Nonnull OperationContext opContext,
            @Nonnull String searchGroup,
            @Nonnull String documentJson,
            @Nonnull String urn) {}

        @Override
        public void deleteDocumentBySearchGroup(
            @Nonnull OperationContext opContext,
            @Nonnull String searchGroup,
            @Nonnull String urn) {}
      };

  /**
   * Upserts a row for {@code urn}. PostgreSQL pgSearch stores at most one row per URN ({@code
   * search_group} is updated with each write); Elasticsearch remains keyed by search group / index.
   * {@code documentJson} is the same payload written to Elasticsearch for the search group.
   */
  void upsertDocumentBySearchGroup(
      @Nonnull OperationContext opContext,
      @Nonnull String searchGroup,
      @Nonnull String documentJson,
      @Nonnull String urn);

  /** Deletes the row for {@code urn} (pgSearch: single row per URN; {@code searchGroup} unused). */
  void deleteDocumentBySearchGroup(
      @Nonnull OperationContext opContext, @Nonnull String searchGroup, @Nonnull String urn);

  /**
   * Appends a run id to the {@code runId} array on the stored document (parity with {@link
   * com.linkedin.metadata.search.elasticsearch.ElasticSearchService#appendRunId}).
   */
  default void appendRunId(
      @Nonnull OperationContext opContext, @Nonnull Urn urn, @Nullable String runId) {
    // optional secondary store
  }

  /**
   * Appends a run id for a V3 search-group document (parity with {@link
   * com.linkedin.metadata.search.elasticsearch.ElasticSearchService#appendRunIdBySearchGroup}).
   */
  default void appendRunIdBySearchGroup(
      @Nonnull OperationContext opContext,
      @Nonnull String searchGroup,
      @Nonnull String docId,
      @Nonnull Urn urn,
      @Nullable String runId) {
    // optional secondary store
  }
}
