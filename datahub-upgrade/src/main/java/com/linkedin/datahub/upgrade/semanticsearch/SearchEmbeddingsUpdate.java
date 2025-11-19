package com.linkedin.datahub.upgrade.semanticsearch;

import com.linkedin.datahub.upgrade.Upgrade;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.search.elasticsearch.update.ESBulkProcessor;
import com.linkedin.metadata.search.embedding.EmbeddingProvider;
import com.linkedin.metadata.search.semantic.EntityTextGenerator;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import javax.annotation.Nonnull;

/**
 * Upgrade job for updating embeddings in semantic search indices.
 *
 * <p>This upgrade generates and updates embeddings for documents that don't have them yet. It
 * assumes documents are already present in semantic indices (via dual-write or out-of-band
 * reindexing).
 *
 * <p>This can be run periodically to incrementally update embeddings as new documents are added.
 */
public class SearchEmbeddingsUpdate implements Upgrade {

  public static final String ENTITY_TYPES_ARG_NAME = "entityTypes";
  public static final String BATCH_SIZE_ARG_NAME = "batchSize";

  private final List<UpgradeStep> _steps;

  public SearchEmbeddingsUpdate(
      @Nonnull OperationContext systemOperationContext,
      @Nonnull SearchClientShim<?> searchClientShim,
      @Nonnull ElasticSearchConfiguration elasticSearchConfiguration,
      @Nonnull EntityTextGenerator entityTextGenerator,
      @Nonnull EmbeddingProvider embeddingProvider,
      @Nonnull ESBulkProcessor esBulkProcessor) {
    this._steps =
        buildSteps(
            systemOperationContext,
            searchClientShim,
            elasticSearchConfiguration,
            entityTextGenerator,
            embeddingProvider,
            esBulkProcessor);
  }

  @Override
  public String id() {
    return "SearchEmbeddingsUpdate";
  }

  @Override
  public List<UpgradeStep> steps() {
    return _steps;
  }

  private List<UpgradeStep> buildSteps(
      @Nonnull OperationContext systemOperationContext,
      @Nonnull SearchClientShim<?> searchClientShim,
      @Nonnull ElasticSearchConfiguration elasticSearchConfiguration,
      @Nonnull EntityTextGenerator entityTextGenerator,
      @Nonnull EmbeddingProvider embeddingProvider,
      @Nonnull ESBulkProcessor esBulkProcessor) {
    return List.of(
        new SearchEmbeddingsUpdateStep(
            systemOperationContext,
            searchClientShim,
            elasticSearchConfiguration,
            entityTextGenerator,
            embeddingProvider,
            esBulkProcessor));
  }
}
