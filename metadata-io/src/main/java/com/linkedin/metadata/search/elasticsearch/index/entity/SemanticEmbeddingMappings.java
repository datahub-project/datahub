package com.linkedin.metadata.search.elasticsearch.index.entity;

import com.google.common.collect.ImmutableMap;
import com.linkedin.metadata.config.search.ModelEmbeddingConfig;
import com.linkedin.metadata.config.search.SemanticSearchConfiguration;
import com.linkedin.metadata.search.elasticsearch.client.shim.builder.es8.Es8SemanticIndexMapper;
import com.linkedin.metadata.search.elasticsearch.client.shim.builder.opensearch2.OpenSearch2SemanticIndexMapper;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim.SearchEngineType;
import com.linkedin.metadata.utils.elasticsearch.shim.SemanticIndexSpec;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Shared embedding field mappings and kNN index-setting dispatch used by V2 semantic sidecar
 * indices and document V3 root embeddings. Engine type comes from the target cluster's {@link
 * SearchClientShim} so split-cluster deploys (V3 on ES, V2 on OpenSearch, or the reverse) stay
 * consistent with that cluster.
 */
public final class SemanticEmbeddingMappings {

  public static final String EMBEDDINGS_FIELD = "embeddings";
  public static final String RESOLVED_TEXT_SHA256_FIELD = "resolvedTextSha256";
  public static final String SKIP_REASON_FIELD = "skipReason";
  public static final String SKIPPED_AT_FIELD = "skippedAt";

  private SemanticEmbeddingMappings() {}

  /**
   * True when semantic search is on, models are configured, and {@code entityName} is in {@code
   * enabledEntities}.
   */
  public static boolean isEnabledForEntity(
      @Nullable SemanticSearchConfiguration semanticConfig, @Nullable String entityName) {
    if (semanticConfig == null
        || !semanticConfig.isEnabled()
        || entityName == null
        || entityName.isEmpty()) {
      return false;
    }
    Set<String> enabledEntities = semanticConfig.getEnabledEntities();
    if (enabledEntities == null || !enabledEntities.contains(entityName)) {
      return false;
    }
    Map<String, ModelEmbeddingConfig> models = semanticConfig.getModels();
    return models != null && !models.isEmpty();
  }

  /**
   * Prefix-independent match for a V3 entity index whose search-group / entity key is {@code
   * entityName} (e.g. {@code documentindex_v3}, {@code prod_documentindex_v3}).
   */
  public static boolean isV3IndexForEntity(@Nullable String indexName, @Nonnull String entityName) {
    if (indexName == null) {
      return false;
    }
    String suffix = entityName + "index_v3";
    return indexName.equals(suffix) || indexName.endsWith("_" + suffix);
  }

  public static boolean isSemanticEnabledV3Index(
      @Nullable SemanticSearchConfiguration semanticConfig, @Nullable String indexName) {
    if (semanticConfig == null
        || !semanticConfig.isEnabled()
        || semanticConfig.getEnabledEntities() == null
        || indexName == null) {
      return false;
    }
    Map<String, ModelEmbeddingConfig> models = semanticConfig.getModels();
    if (models == null || models.isEmpty()) {
      return false;
    }
    for (String entityName : semanticConfig.getEnabledEntities()) {
      if (isV3IndexForEntity(indexName, entityName)) {
        return true;
      }
    }
    return false;
  }

  /**
   * OpenSearch (including OS 3) uses index-level {@code knn: true}. Elasticsearch 8/9 reject that
   * setting because {@code dense_vector} handles kNN at the field level.
   */
  public static boolean shouldEnableIndexLevelKnn(@Nullable SearchClientShim<?> searchClientShim) {
    if (searchClientShim == null) {
      return true;
    }
    return searchClientShim.getEngineType().isOpenSearch();
  }

  @Nonnull
  public static Map<String, Object> provenanceRootMappings() {
    return ImmutableMap.of(
        RESOLVED_TEXT_SHA256_FIELD,
        ImmutableMap.of("type", "keyword"),
        SKIP_REASON_FIELD,
        ImmutableMap.of("type", "keyword"),
        SKIPPED_AT_FIELD,
        ImmutableMap.of("type", "date"));
  }

  /**
   * Builds the {@code embeddings} field mapping ({@code properties} keyed by model) using the same
   * mappers as the V2 semantic sidecar.
   */
  @Nonnull
  @SuppressWarnings("unchecked")
  public static Map<String, Object> buildEmbeddingFieldConfig(
      @Nonnull SemanticSearchConfiguration semanticConfig,
      @Nullable SearchClientShim<?> searchClientShim) {
    SearchEngineType engineType =
        searchClientShim != null ? searchClientShim.getEngineType() : SearchEngineType.OPENSEARCH_2;

    Map<String, Object> modelProperties = new HashMap<>();

    for (Map.Entry<String, ModelEmbeddingConfig> entry : semanticConfig.getModels().entrySet()) {
      String modelKey = entry.getKey();
      ModelEmbeddingConfig modelConfig = entry.getValue();

      String translatedSimilarity = translateSpaceType(modelConfig.getSpaceType(), engineType);

      SemanticIndexSpec spec =
          SemanticIndexSpec.builder()
              .indexName("semantic")
              .modelKey(modelKey)
              .vectorDimension(modelConfig.getVectorDimension())
              .similarity(translatedSimilarity)
              .hnswM(modelConfig.getM())
              .hnswEfConstruction(modelConfig.getEfConstruction())
              .knnEngine(modelConfig.getKnnEngine())
              .build();

      Map<String, Object> fullMapping;
      if (usesEs8VectorMappings(engineType)) {
        fullMapping = Es8SemanticIndexMapper.build(spec);
      } else {
        fullMapping = OpenSearch2SemanticIndexMapper.build(spec);
      }

      Map<String, Object> topProps = (Map<String, Object>) fullMapping.get("properties");
      Map<String, Object> embeddingsField = (Map<String, Object>) topProps.get("embeddings");
      Map<String, Object> embeddingsProps = (Map<String, Object>) embeddingsField.get("properties");
      Map<String, Object> modelEntry = (Map<String, Object>) embeddingsProps.get(modelKey);

      Map<String, Object> modelEntryProps =
          new HashMap<>((Map<String, Object>) modelEntry.get("properties"));
      modelEntryProps.put("sourceTextSha256", ImmutableMap.of("type", "keyword"));
      Map<String, Object> augmentedModelEntry = new HashMap<>(modelEntry);
      augmentedModelEntry.put("properties", modelEntryProps);

      modelProperties.put(modelKey, augmentedModelEntry);
    }

    return ImmutableMap.of("properties", modelProperties);
  }

  static boolean usesEs8VectorMappings(@Nonnull SearchEngineType engine) {
    return engine.requiresEs8JavaClient();
  }

  /**
   * Translates a space-type string between OpenSearch and Elasticsearch 8 vocabulary.
   *
   * <p>OpenSearch uses {@code cosinesimil} / {@code l2} / {@code innerproduct}; ES 8 uses {@code
   * cosine} / {@code l2_norm} / {@code dot_product}. Because application.yaml ships model configs
   * for both engines, the configured value may be in either vocabulary — this method normalises it
   * for the target engine in both directions.
   */
  static String translateSpaceType(@Nonnull String spaceType, @Nonnull SearchEngineType engine) {
    if (usesEs8VectorMappings(engine)) {
      return switch (spaceType) {
        case "cosinesimil" -> "cosine";
        case "l2" -> "l2_norm";
        case "innerproduct", "dotproduct" -> "dot_product";
        default -> spaceType;
      };
    }
    return switch (spaceType) {
      case "cosine" -> "cosinesimil";
      case "l2_norm" -> "l2";
      case "dot_product" -> "innerproduct";
      default -> spaceType;
    };
  }
}
