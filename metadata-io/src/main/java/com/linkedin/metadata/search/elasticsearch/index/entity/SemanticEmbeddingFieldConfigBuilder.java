package com.linkedin.metadata.search.elasticsearch.index.entity;

import com.linkedin.metadata.config.search.ModelEmbeddingConfig;
import com.linkedin.metadata.config.search.SemanticSearchConfiguration;
import com.linkedin.metadata.search.elasticsearch.client.shim.builder.es8.Es8SemanticIndexMapper;
import com.linkedin.metadata.search.elasticsearch.client.shim.builder.opensearch2.OpenSearch2SemanticIndexMapper;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import com.linkedin.metadata.utils.elasticsearch.shim.SemanticIndexSpec;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/** Builds the engine-specific {@code embeddings.<modelKey>.chunks[]} mapping subtree. */
public final class SemanticEmbeddingFieldConfigBuilder {

  private SemanticEmbeddingFieldConfigBuilder() {}

  public static boolean hasEmbeddingModels(
      @Nullable final SemanticSearchConfiguration semanticConfig) {
    return semanticConfig != null
        && semanticConfig.getModels() != null
        && !semanticConfig.getModels().isEmpty();
  }

  /**
   * Whether V3 consolidated indices should carry embedding vector mappings (and the index-level KNN
   * setting). Model entries exist in the default configuration even when semantic search is off, so
   * the presence of models alone must not add vector fields: a keyword-only V3 rollout would
   * otherwise emit {@code knn_vector} mappings that unsupported engines (e.g. Elasticsearch 7)
   * reject at index creation. When semantic search is enabled, the startup engine-version check
   * rejects unsupported engines before index construction.
   */
  public static boolean semanticIndexingEnabled(
      @Nullable final SemanticSearchConfiguration semanticConfig) {
    return semanticConfig != null
        && semanticConfig.isEnabled()
        && hasEmbeddingModels(semanticConfig);
  }

  @Nonnull
  @SuppressWarnings("unchecked")
  public static Map<String, Object> buildEmbeddingFieldConfig(
      @Nonnull final SemanticSearchConfiguration semanticConfig,
      @Nullable final SearchClientShim<?> searchClientShim) {
    final SearchClientShim.SearchEngineType engineType =
        searchClientShim != null
            ? searchClientShim.getEngineType()
            : SearchClientShim.SearchEngineType.OPENSEARCH_2;
    final Map<String, Object> modelProperties = new LinkedHashMap<>();

    for (Map.Entry<String, ModelEmbeddingConfig> entry :
        new TreeMap<>(semanticConfig.getModels()).entrySet()) {
      final String modelKey = entry.getKey();
      final ModelEmbeddingConfig modelConfig = entry.getValue();
      final String translatedSimilarity =
          translateSpaceType(modelConfig.getSpaceType(), engineType);

      final SemanticIndexSpec spec =
          SemanticIndexSpec.builder()
              .indexName("semantic")
              .modelKey(modelKey)
              .vectorDimension(modelConfig.getVectorDimension())
              .similarity(translatedSimilarity)
              .hnswM(modelConfig.getM())
              .hnswEfConstruction(modelConfig.getEfConstruction())
              .knnEngine(modelConfig.getKnnEngine().trim().toLowerCase(Locale.ROOT))
              .build();

      final Map<String, Object> fullMapping =
          usesDenseVector(engineType)
              ? Es8SemanticIndexMapper.build(spec)
              : OpenSearch2SemanticIndexMapper.build(spec);

      // Both mappers emit {properties: {embeddings: {properties: {<modelKey>: ...}}}}.
      final Map<String, Object> topProps = (Map<String, Object>) fullMapping.get("properties");
      final Map<String, Object> embeddingsField = (Map<String, Object>) topProps.get("embeddings");
      final Map<String, Object> embeddingsProps =
          (Map<String, Object>) embeddingsField.get("properties");
      modelProperties.put(modelKey, embeddingsProps.get(modelKey));
    }

    return Map.of("properties", modelProperties);
  }

  private static boolean usesDenseVector(
      @Nonnull final SearchClientShim.SearchEngineType engineType) {
    return engineType.requiresEs8JavaClient();
  }

  /**
   * Translates OpenSearch and Elasticsearch 8 vector similarity vocabulary in both directions. The
   * application config may contain either vocabulary because deployments can switch engines.
   */
  private static String translateSpaceType(
      @Nonnull final String spaceType,
      @Nonnull final SearchClientShim.SearchEngineType engineType) {
    final String canonicalSpaceType = SemanticEmbeddingMappings.canonicalSpaceType(spaceType);
    if (usesDenseVector(engineType)) {
      return switch (canonicalSpaceType) {
        case "cosinesimil" -> "cosine";
        case "l2" -> "l2_norm";
        case "innerproduct", "dotproduct" -> "dot_product";
        default -> canonicalSpaceType;
      };
    }
    return switch (canonicalSpaceType) {
      case "cosine" -> "cosinesimil";
      case "l2_norm" -> "l2";
      case "dotproduct", "dot_product" -> "innerproduct";
      default -> canonicalSpaceType;
    };
  }
}
