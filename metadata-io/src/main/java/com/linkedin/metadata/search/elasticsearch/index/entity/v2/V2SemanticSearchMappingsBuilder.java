package com.linkedin.metadata.search.elasticsearch.index.entity.v2;

import com.google.common.collect.ImmutableMap;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.config.search.ModelEmbeddingConfig;
import com.linkedin.metadata.config.search.SemanticSearchConfiguration;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.search.elasticsearch.client.shim.builder.es8.Es8SemanticIndexMapper;
import com.linkedin.metadata.search.elasticsearch.client.shim.builder.opensearch2.OpenSearch2SemanticIndexMapper;
import com.linkedin.metadata.search.elasticsearch.index.MappingsBuilder;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import com.linkedin.metadata.utils.elasticsearch.shim.SemanticIndexSpec;
import com.linkedin.structured.StructuredPropertyDefinition;
import com.linkedin.util.Pair;
import io.datahubproject.metadata.context.OperationContext;
import java.util.*;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

/**
 * Mappings builder for semantic search indices that extends V2 mappings with vector embeddings
 * support. Dispatches to the appropriate engine-specific mapper (ES 8 or OpenSearch 2) so that the
 * correct vector field type ({@code dense_vector} vs {@code knn_vector}) is used for the target
 * cluster.
 *
 * <p>This builder creates additional indices (with "_semantic" suffix) for entities configured in
 * SemanticSearchConfiguration. These indices include all standard V2 mappings plus an "embeddings"
 * field for storing vector embeddings used in semantic search.
 */
@Slf4j
public class V2SemanticSearchMappingsBuilder implements MappingsBuilder {
  private final MappingsBuilder v2MappingsBuilder;
  private final SemanticSearchConfiguration semanticConfig;
  private final IndexConvention indexConvention;
  private final SearchClientShim<?> searchClientShim;

  /** Production constructor — engine type is derived from the injected shim at runtime. */
  public V2SemanticSearchMappingsBuilder(
      @Nonnull MappingsBuilder v2MappingsBuilder,
      @Nonnull SemanticSearchConfiguration semanticConfig,
      @Nonnull IndexConvention indexConvention,
      @Nonnull SearchClientShim<?> searchClientShim) {
    this.v2MappingsBuilder = v2MappingsBuilder;
    this.semanticConfig = semanticConfig;
    this.indexConvention = indexConvention;
    this.searchClientShim = searchClientShim;
  }

  /**
   * Backwards-compatible constructor for callers that don't yet pass a shim (defaults to OS 2
   * behaviour, i.e. {@code knn_vector}).
   *
   * @deprecated Inject a {@link SearchClientShim} so the builder produces the correct field type
   *     for the target engine.
   */
  @Deprecated
  public V2SemanticSearchMappingsBuilder(
      @Nonnull MappingsBuilder v2MappingsBuilder,
      @Nonnull SemanticSearchConfiguration semanticConfig,
      @Nonnull IndexConvention indexConvention) {
    this(v2MappingsBuilder, semanticConfig, indexConvention, null);
  }

  /**
   * Translates a space-type string between OpenSearch and Elasticsearch 8 vocabulary.
   *
   * <p>OpenSearch uses {@code cosinesimil} / {@code l2} / {@code innerproduct}; ES 8 uses {@code
   * cosine} / {@code l2_norm} / {@code dot_product}. Because application.yaml ships model configs
   * for both engines, the configured value may be in either vocabulary — this method normalises it
   * for the target engine in both directions.
   */
  private static String translateSpaceType(
      @Nonnull String spaceType, @Nonnull SearchClientShim.SearchEngineType engine) {
    if (engine == SearchClientShim.SearchEngineType.ELASTICSEARCH_8) {
      return switch (spaceType) {
        case "cosinesimil" -> "cosine";
        case "l2" -> "l2_norm";
        case "innerproduct", "dotproduct" -> "dot_product";
        default -> spaceType;
      };
    }
    // OpenSearch 2 path — reverse-translate ES 8 vocabulary if present
    return switch (spaceType) {
      case "cosine" -> "cosinesimil";
      case "l2_norm" -> "l2";
      case "dot_product" -> "innerproduct";
      default -> spaceType;
    };
  }

  /**
   * Builds the embedding field configuration dynamically from SemanticSearchConfiguration.
   *
   * <p>Supports multiple embedding models with different configurations. Dispatches to the
   * engine-specific mapper so that ES 8 deployments use {@code dense_vector} while OpenSearch
   * deployments continue to use {@code knn_vector}.
   *
   * <p>Structure returned:
   *
   * <pre>{@code
   * {
   *   "properties": {
   *     "<model_key>": {  // e.g., "cohere_embed_v3"
   *       "properties": {
   *         "chunks": {
   *           "type": "nested",
   *           "properties": {
   *             "vector": { "type": "dense_vector"|"knn_vector", "dimension": 1024, ... },
   *             "text": { "type": "text", "index": false },
   *             "position": { "type": "integer" },
   *             "characterOffset": { "type": "integer" },
   *             "characterLength": { "type": "integer" },
   *             "tokenCount": { "type": "integer" }
   *           }
   *         },
   *         "totalChunks": { "type": "integer" },
   *         "modelVersion": { "type": "keyword" },
   *         "generatedAt": { "type": "date" }
   *       }
   *     }
   *   }
   * }
   * }</pre>
   *
   * @return Map representing the mapping for the embeddings field
   */
  @SuppressWarnings("unchecked")
  private Map<String, Object> buildEmbeddingFieldConfig() {
    SearchClientShim.SearchEngineType engineType =
        searchClientShim != null
            ? searchClientShim.getEngineType()
            : SearchClientShim.SearchEngineType.OPENSEARCH_2;

    Map<String, Object> modelProperties = new HashMap<>();

    for (Map.Entry<String, ModelEmbeddingConfig> entry : semanticConfig.getModels().entrySet()) {
      String modelKey = entry.getKey();
      ModelEmbeddingConfig modelConfig = entry.getValue();

      String translatedSimilarity = translateSpaceType(modelConfig.getSpaceType(), engineType);

      SemanticIndexSpec spec =
          SemanticIndexSpec.builder()
              .indexName("semantic") // not used at this layer
              .modelKey(modelKey)
              .vectorDimension(modelConfig.getVectorDimension())
              .similarity(translatedSimilarity)
              .hnswM(modelConfig.getM())
              .hnswEfConstruction(modelConfig.getEfConstruction())
              .knnEngine(modelConfig.getKnnEngine())
              .build();

      Map<String, Object> fullMapping;
      if (engineType == SearchClientShim.SearchEngineType.ELASTICSEARCH_8) {
        fullMapping = Es8SemanticIndexMapper.build(spec);
      } else {
        fullMapping = OpenSearch2SemanticIndexMapper.build(spec);
      }

      // Both mappers produce: {properties: {urn: ..., embeddings: {properties: {<modelKey>: ...}}}}
      // Extract just the modelKey subtree from embeddings.properties
      Map<String, Object> topProps = (Map<String, Object>) fullMapping.get("properties");
      Map<String, Object> embeddingsField = (Map<String, Object>) topProps.get("embeddings");
      Map<String, Object> embeddingsProps = (Map<String, Object>) embeddingsField.get("properties");
      Map<String, Object> modelEntry = (Map<String, Object>) embeddingsProps.get(modelKey);

      modelProperties.put(modelKey, modelEntry);
    }

    return ImmutableMap.of("properties", modelProperties);
  }

  /**
   * Transforms base V2 index mappings into semantic search index mappings by adding embeddings
   * field.
   *
   * @param baseIndexMappings Base V2 index mappings to transform
   * @return Semantic search index mappings with embeddings field added
   */
  private Collection<IndexMapping> addSemanticMappings(Collection<IndexMapping> baseIndexMappings) {
    Set<String> enabledEntities = semanticConfig.getEnabledEntities();
    Map<String, Object> embeddingFieldConfig = buildEmbeddingFieldConfig();
    ArrayList<IndexMapping> semanticIndexMappings = new ArrayList<>();

    for (IndexMapping baseIndexMapping : baseIndexMappings) {
      String indexName = baseIndexMapping.getIndexName();
      String entityName = indexConvention.getEntityName(indexName).orElse(null);

      // Only create semantic search index for enabled entities
      if (!enabledEntities.contains(entityName)) {
        log.debug(
            "Skipping semantic search index for entity '{}' (not in enabled list: {})",
            entityName,
            enabledEntities);
        continue;
      }

      log.debug(
          "Creating semantic search index mapping for entity '{}' (index: {})",
          entityName,
          indexName);
      Map<String, Object> baseMappings = baseIndexMapping.getMappings();
      @SuppressWarnings("unchecked")
      Map<String, Object> basePropertiesMap = (Map<String, Object>) baseMappings.get("properties");

      // Construct new properties map with "embeddings" field
      ImmutableMap.Builder<String, Object> newPropertiesMap = new ImmutableMap.Builder<>();
      newPropertiesMap.putAll(basePropertiesMap);
      newPropertiesMap.put("embeddings", embeddingFieldConfig);

      // Construct new top-level map with new properties map
      ImmutableMap.Builder<String, Object> newMappings = new ImmutableMap.Builder<>();
      newMappings.putAll(baseMappings);
      newMappings.put("properties", newPropertiesMap.build());

      // Construct new IndexMapping object for semantic search
      Map<String, Object> finalMappings = newMappings.buildKeepingLast();

      String semanticIndexName = indexConvention.getEntityIndexNameSemantic(entityName);
      IndexMapping semanticIndexMapping =
          IndexMapping.builder().indexName(semanticIndexName).mappings(finalMappings).build();

      semanticIndexMappings.add(semanticIndexMapping);
    }

    return semanticIndexMappings;
  }

  @Override
  public Collection<IndexMapping> getIndexMappings(@Nonnull OperationContext opContext) {
    return getIndexMappings(opContext, List.of());
  }

  @Override
  public Collection<IndexMapping> getIndexMappings(
      @Nonnull OperationContext opContext,
      @Nonnull Collection<Pair<Urn, StructuredPropertyDefinition>> structuredProperties) {
    Collection<IndexMapping> baseIndexMappings =
        v2MappingsBuilder.getIndexMappings(opContext, structuredProperties);
    return addSemanticMappings(baseIndexMappings);
  }

  @Override
  public Collection<IndexMapping> getIndexMappingsWithNewStructuredProperty(
      @Nonnull OperationContext opContext,
      @Nonnull Urn urn,
      @Nonnull StructuredPropertyDefinition property) {
    Collection<IndexMapping> baseIndexMappings =
        v2MappingsBuilder.getIndexMappingsWithNewStructuredProperty(opContext, urn, property);
    return addSemanticMappings(baseIndexMappings);
  }

  @Override
  public Map<String, Object> getIndexMappingsForStructuredProperty(
      Collection<Pair<Urn, StructuredPropertyDefinition>> properties) {
    // Semantic search indices use the same structured property field type mappings as V2
    return v2MappingsBuilder.getIndexMappingsForStructuredProperty(properties);
  }

  @Override
  public Map<String, Object> getIndexMappings(
      @Nonnull EntityRegistry entityRegistry, @Nonnull EntitySpec entitySpec) {
    // Semantic search indices use the same field type mappings as V2 for field type extraction.
    // This method is called to extract field type information for query filtering.
    return v2MappingsBuilder.getIndexMappings(entityRegistry, entitySpec);
  }
}
