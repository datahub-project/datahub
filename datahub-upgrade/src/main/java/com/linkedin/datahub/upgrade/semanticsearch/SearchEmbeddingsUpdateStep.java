package com.linkedin.datahub.upgrade.semanticsearch;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeStepResult;
import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.config.search.ModelEmbeddingConfig;
import com.linkedin.metadata.search.elasticsearch.index.entity.v2.EmbeddingUtils;
import com.linkedin.metadata.search.elasticsearch.update.ESBulkProcessor;
import com.linkedin.metadata.search.embedding.EmbeddingProvider;
import com.linkedin.metadata.search.semantic.EntityTextGenerator;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.datahubproject.metadata.context.OperationContext;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import javax.annotation.Nonnull;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.SearchHit;
import org.opensearch.search.builder.SearchSourceBuilder;

/** Upgrade step that updates embeddings for documents in semantic search indices. */
@Slf4j
public class SearchEmbeddingsUpdateStep implements UpgradeStep {

  private static final int DEFAULT_BATCH_SIZE = 100;

  private final OperationContext operationContext;
  private final SearchClientShim<?> searchClientShim;
  private final ElasticSearchConfiguration elasticSearchConfiguration;
  private final EntityTextGenerator entityTextGenerator;
  private final EmbeddingProvider embeddingProvider;
  private final ESBulkProcessor esBulkProcessor;

  public SearchEmbeddingsUpdateStep(
      OperationContext systemOperationContext,
      SearchClientShim<?> searchClientShim,
      ElasticSearchConfiguration elasticSearchConfiguration,
      EntityTextGenerator entityTextGenerator,
      EmbeddingProvider embeddingProvider,
      ESBulkProcessor esBulkProcessor) {
    this.operationContext = systemOperationContext;
    this.searchClientShim = searchClientShim;
    this.elasticSearchConfiguration = elasticSearchConfiguration;
    this.entityTextGenerator = entityTextGenerator;
    this.embeddingProvider = embeddingProvider;
    this.esBulkProcessor = esBulkProcessor;
  }

  @Override
  public String id() {
    return "SearchEmbeddingsUpdateStep";
  }

  /** Helper to report progress to upgrade context. */
  private static void report(UpgradeContext context, String format, Object... args) {
    context.report().addLine(String.format(format, args));
  }

  @Override
  public Function<UpgradeContext, UpgradeStepResult> executable() {
    return this::execute;
  }

  @Nonnull
  private DefaultUpgradeStepResult execute(UpgradeContext context) {
    try {
      report(context, "Starting embedding backfill for semantic search indices...");

      // Get configuration
      int batchSize = getBatchSize(context);
      Set<String> entityTypes = getEntityTypesToProcess(context);
      Map<String, ModelEmbeddingConfig> models = getConfiguredModels();

      if (models.isEmpty()) {
        report(context, "No embedding models configured, skipping");
        return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.SUCCEEDED);
      }

      report(
          context,
          "Configuration: batchSize=%d, entityTypes=%s, models=%s",
          batchSize,
          entityTypes,
          models.keySet());

      int totalProcessed = 0;
      int totalUpdated = 0;
      int totalFailed = 0;

      for (String entityType : entityTypes) {
        try {
          report(context, "Processing entity type: %s", entityType);

          String indexName =
              operationContext
                  .getSearchContext()
                  .getIndexConvention()
                  .getEntityIndexNameSemantic(entityType);
          report(context, "  Index: %s", indexName);

          // Check if index exists
          if (!indexExists(indexName)) {
            report(context, "  ⚠️  Index %s does not exist, skipping", indexName);
            continue;
          }

          // Process each model for this entity type
          for (Map.Entry<String, ModelEmbeddingConfig> modelEntry : models.entrySet()) {
            String modelKey = modelEntry.getKey();
            String modelVersion = modelKey.replace("_", ".");

            report(context, "  Processing model: %s", modelKey);

            // Process batches for this entity type + model combination
            UpdateEmbeddingsResult result =
                updateEmbeddingsForEntityType(
                    context, entityType, indexName, batchSize, modelKey, modelVersion);

            totalProcessed += result.getProcessed();
            totalUpdated += result.getUpdated();
            totalFailed += result.getFailed();

            report(
                context,
                "  ✅ Completed %s/%s: %d processed, %d updated, %d failed",
                entityType,
                modelKey,
                result.getProcessed(),
                result.getUpdated(),
                result.getFailed());
          }

        } catch (Exception e) {
          log.error("Failed to backfill embeddings for entity type: {}", entityType, e);
          report(context, "  ❌ Failed to process %s: %s", entityType, e.getMessage());
        }
      }

      report(
          context,
          "Embedding backfill completed: %d processed, %d updated, %d failed",
          totalProcessed,
          totalUpdated,
          totalFailed);

      if (totalFailed > 0) {
        report(context, "⚠️  Warning: %d documents failed to process", totalFailed);
      }

      return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.SUCCEEDED);
    } catch (Exception e) {
      log.error("Error during BackfillEmbeddingsStep execution", e);
      report(context, "Error during execution: %s", e.getMessage());
      return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.FAILED);
    }
  }

  /** Update embeddings for a single entity type and model. */
  private UpdateEmbeddingsResult updateEmbeddingsForEntityType(
      UpgradeContext context,
      String entityType,
      String indexName,
      int batchSize,
      String modelKey,
      String modelVersion) {

    UpdateEmbeddingsResult result = new UpdateEmbeddingsResult();
    Set<String> processedDocIds = new HashSet<>();
    int batchNum = 0;

    try {
      while (true) {
        try {
          batchNum++;
          report(context, "    Batch %d...", batchNum);

          // Fetch documents without embeddings for this specific model
          List<DocumentWithId> documents =
              fetchDocumentsWithoutEmbeddings(indexName, batchSize, processedDocIds, modelKey);

          if (documents.isEmpty()) {
            report(context, "    No more documents to process");
            break;
          }
          report(context, "    Fetched %d documents", documents.size());

          // Process batch
          BatchProcessingResult batchResult =
              processBatch(entityType, indexName, documents, modelKey, modelVersion);

          result.addProcessed(batchResult.getProcessed());
          result.addUpdated(batchResult.getUpdated());
          result.addFailed(batchResult.getFailed());

          // Track processed IDs to avoid infinite loops
          for (DocumentWithId doc : documents) {
            processedDocIds.add(doc.getId());
          }

          // Flush after each batch to ensure updates are applied
          esBulkProcessor.flush();

          report(
              context,
              "    Batch complete: %d updated, %d failed",
              batchResult.getUpdated(),
              batchResult.getFailed());

        } catch (Exception e) {
          log.error("Error processing batch {} for {}", batchNum, entityType, e);
          report(context, "    ❌ Batch %d failed: %s", batchNum, e.getMessage());
          break;
        }
      }

    } finally {
      // Ensure all pending updates are flushed and bulk processor is closed
      try {
        esBulkProcessor.flush();
      } catch (Exception e) {
        log.error("Error flushing bulk processor", e);
      }
    }

    return result;
  }

  /** Process a batch of documents: generate text, create embeddings, update documents. */
  private BatchProcessingResult processBatch(
      String entityType,
      String indexName,
      List<DocumentWithId> documents,
      String modelKey,
      String modelVersion) {

    BatchProcessingResult result = new BatchProcessingResult();
    for (DocumentWithId doc : documents) {
      result.incrementProcessed();

      try {
        // Generate text description
        String text = entityTextGenerator.generateText(entityType, doc.getSource());
        if (text.trim().isEmpty()) {
          log.warn("Empty text generated for document: {}", doc.getId());
          result.incrementFailed();
          continue;
        }

        // Generate embedding
        // TODO: Make model configurable
        float[] embedding = embeddingProvider.embed(text, null);

        // Update document with embedding for this specific model
        updateDocumentWithEmbedding(
            indexName, doc.getId(), text, embedding, modelKey, modelVersion);
        result.incrementUpdated();
      } catch (Exception e) {
        log.error("Error processing document {}: {}", doc.getId(), e.getMessage());
        result.incrementFailed();
      }
    }

    return result;
  }

  /** Update a document with embedding data for a specific model. */
  private void updateDocumentWithEmbedding(
      String indexName,
      String docId,
      String text,
      float[] embedding,
      String modelKey,
      String modelVersion) {
    try {
      // Build the embedding structure matching the schema
      ObjectMapper objectMapper = operationContext.getObjectMapper();
      ObjectNode embeddingData =
          EmbeddingUtils.createEmbeddingUpdate(
              objectMapper, embedding, text, modelKey, modelVersion);

      // Create update request to merge embeddings field into existing document
      UpdateRequest updateRequest =
          new UpdateRequest(indexName, docId)
              .detectNoop(false)
              .retryOnConflict(3)
              .doc(objectMapper.writeValueAsBytes(embeddingData), XContentType.JSON);

      esBulkProcessor.add(updateRequest);
    } catch (Exception e) {
      log.error("Failed to update document {} with embedding", docId, e);
      throw new RuntimeException(e);
    }
  }

  /** Fetch documents that don't have embeddings for a specific model. */
  private List<DocumentWithId> fetchDocumentsWithoutEmbeddings(
      String indexName, int batchSize, Set<String> processedDocIds, String modelKey) {
    try {
      // Build query: documents without embeddings for this specific model
      BoolQueryBuilder boolQuery =
          QueryBuilders.boolQuery().mustNot(QueryBuilders.existsQuery("embeddings." + modelKey));

      // Exclude already processed documents to avoid infinite loops
      if (!processedDocIds.isEmpty()) {
        boolQuery.mustNot(QueryBuilders.idsQuery().addIds(processedDocIds.toArray(new String[0])));
      }

      SearchRequest searchRequest =
          new SearchRequest(indexName)
              .source(new SearchSourceBuilder().query(boolQuery).size(batchSize).fetchSource(true));
      SearchResponse response = searchClientShim.search(searchRequest, RequestOptions.DEFAULT);

      List<DocumentWithId> documents = new java.util.ArrayList<>();
      for (SearchHit hit : response.getHits().getHits()) {
        try {
          JsonNode source = operationContext.getObjectMapper().readTree(hit.getSourceAsString());
          documents.add(new DocumentWithId(hit.getId(), source));
        } catch (Exception e) {
          log.warn("Failed to parse document {}: {}", hit.getId(), e.getMessage());
        }
      }

      return documents;
    } catch (Exception e) {
      log.error("Error fetching documents from index: {}", indexName, e);
      return List.of();
    }
  }

  private Set<String> getEntityTypesToProcess(UpgradeContext context) {
    Optional<String> entityTypesArg =
        context
            .parsedArgs()
            .getOrDefault(SearchEmbeddingsUpdate.ENTITY_TYPES_ARG_NAME, Optional.empty());

    if (entityTypesArg.isPresent() && !entityTypesArg.get().isEmpty()) {
      return Set.of(entityTypesArg.get().split(","));
    }

    if (elasticSearchConfiguration.getEntityIndex() != null
        && elasticSearchConfiguration.getEntityIndex().getSemanticSearch() != null) {
      Set<String> enabledEntities =
          elasticSearchConfiguration.getEntityIndex().getSemanticSearch().getEnabledEntities();
      if (!enabledEntities.isEmpty()) {
        return enabledEntities;
      }
    }
    return Set.of();
  }

  private int getBatchSize(UpgradeContext context) {
    Optional<String> batchSizeArg =
        context
            .parsedArgs()
            .getOrDefault(SearchEmbeddingsUpdate.BATCH_SIZE_ARG_NAME, Optional.empty());

    if (batchSizeArg.isPresent()) {
      try {
        return Integer.parseInt(batchSizeArg.get());
      } catch (NumberFormatException e) {
        log.warn("Invalid batch size arg, using default");
      }
    }

    if (elasticSearchConfiguration.getEntityIndex() != null
        && elasticSearchConfiguration.getEntityIndex().getSemanticSearch() != null
        && elasticSearchConfiguration.getEntityIndex().getSemanticSearch().getEmbeddingsUpdate()
            != null) {
      return elasticSearchConfiguration
          .getEntityIndex()
          .getSemanticSearch()
          .getEmbeddingsUpdate()
          .getBatchSize();
    }
    return DEFAULT_BATCH_SIZE;
  }

  private boolean indexExists(String indexName) {
    try {
      return searchClientShim.indexExists(new GetIndexRequest(indexName), RequestOptions.DEFAULT);
    } catch (Exception e) {
      log.error("Error checking if index exists: {}", indexName, e);
      return false;
    }
  }

  private Map<String, ModelEmbeddingConfig> getConfiguredModels() {
    if (elasticSearchConfiguration.getEntityIndex() != null
        && elasticSearchConfiguration.getEntityIndex().getSemanticSearch() != null) {
      return elasticSearchConfiguration.getEntityIndex().getSemanticSearch().getModels();
    }
    return Map.of();
  }

  /** Simple container for document ID and source data */
  private static class DocumentWithId {
    private final String id;
    private final JsonNode source;

    public DocumentWithId(String id, JsonNode source) {
      this.id = id;
      this.source = source;
    }

    public String getId() {
      return id;
    }

    public JsonNode getSource() {
      return source;
    }
  }

  @Getter
  private static class UpdateEmbeddingsResult {
    private int processed = 0;
    private int updated = 0;
    private int failed = 0;

    public void addProcessed(int count) {
      processed += count;
    }

    public void addUpdated(int count) {
      updated += count;
    }

    public void addFailed(int count) {
      failed += count;
    }
  }

  /** Result tracking for batch processing */
  @Getter
  private static class BatchProcessingResult {
    private int processed = 0;
    private int updated = 0;
    private int failed = 0;

    public void incrementProcessed() {
      processed++;
    }

    public void incrementUpdated() {
      updated++;
    }

    public void incrementFailed() {
      failed++;
    }
  }
}
