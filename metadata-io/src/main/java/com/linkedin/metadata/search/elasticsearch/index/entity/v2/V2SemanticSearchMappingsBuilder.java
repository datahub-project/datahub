package com.linkedin.metadata.search.elasticsearch.index.entity.v2;

import com.google.common.collect.ImmutableMap;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.config.search.ModelEmbeddingConfig;
import com.linkedin.metadata.config.search.SemanticSearchConfiguration;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.search.elasticsearch.index.MappingsBuilder;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.structured.StructuredPropertyDefinition;
import com.linkedin.util.Pair;
import io.datahubproject.metadata.context.OperationContext;
import java.util.*;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

/**
 * Mappings builder for semantic search indices that extends V2 mappings with vector embeddings
 * support.
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

  public V2SemanticSearchMappingsBuilder(
      @Nonnull MappingsBuilder v2MappingsBuilder,
      @Nonnull SemanticSearchConfiguration semanticConfig,
      @Nonnull IndexConvention indexConvention) {
    this.v2MappingsBuilder = v2MappingsBuilder;
    this.semanticConfig = semanticConfig;
    this.indexConvention = indexConvention;
  }

  /**
   * Builds the embedding field configuration dynamically from SemanticSearchConfiguration.
   *
   * <p>Supports multiple embedding models with different configurations. Structure:
   *
   * <pre>{@code
   * {
   *   "properties": {
   *     "<model_key>": {  // e.g., "cohere_embed_v3"
   *       "properties": {
   *         "chunks": {
   *           "type": "nested",
   *           "properties": {
   *             "vector": { "type": "knn_vector", "dimension": 1024, "method": {...} },
   *             "text": { "type": "text", "index": false },
   *             "position": { "type": "integer" },
   *             "characterOffset": { "type": "integer" },
   *             "characterLength": { "type": "integer" },
   *             "tokenCount": { "type": "integer" }
   *           }
   *         },
   *         "totalChunks": { "type": "integer" },
   *         "totalTokens": { "type": "integer" }
   *       }
   *     }
   *   }
   * }
   * }</pre>
   *
   * @return Map representing the mapping for the embeddings field
   */
  private Map<String, Object> buildEmbeddingFieldConfig() {
    Map<String, Object> modelProperties = new HashMap<>();

    // Iterate over each configured model
    for (Map.Entry<String, ModelEmbeddingConfig> entry : semanticConfig.getModels().entrySet()) {
      String modelKey = entry.getKey();
      ModelEmbeddingConfig modelConfig = entry.getValue();

      // Build k-NN method configuration for this model
      Map<String, Object> methodParams = new HashMap<>();
      methodParams.put("ef_construction", modelConfig.getEfConstruction());
      methodParams.put("m", modelConfig.getM());

      Map<String, Object> method = new HashMap<>();
      method.put("name", "hnsw");
      method.put("space_type", modelConfig.getSpaceType());
      method.put("engine", modelConfig.getKnnEngine());
      method.put("parameters", methodParams);

      // Build vector field with k-NN configuration
      Map<String, Object> vectorField = new HashMap<>();
      vectorField.put("type", "knn_vector");
      vectorField.put("dimension", modelConfig.getVectorDimension());
      vectorField.put("method", method);

      // Build text field (not indexed, for reference only)
      Map<String, Object> textField = new HashMap<>();
      textField.put("type", "text");
      textField.put("index", false);

      // Build position, characterOffset/characterLength, and tokenCount fields
      Map<String, Object> positionField = ImmutableMap.of("type", "integer");
      Map<String, Object> characterOffsetField = ImmutableMap.of("type", "integer");
      Map<String, Object> characterLengthField = ImmutableMap.of("type", "integer");
      Map<String, Object> tokenCountField = ImmutableMap.of("type", "integer");

      // Build chunks nested structure
      Map<String, Object> chunksProperties = new HashMap<>();
      chunksProperties.put("vector", vectorField);
      chunksProperties.put("text", textField);
      chunksProperties.put("position", positionField);
      chunksProperties.put("characterOffset", characterOffsetField);
      chunksProperties.put("characterLength", characterLengthField);
      chunksProperties.put("tokenCount", tokenCountField);

      Map<String, Object> chunksField = new HashMap<>();
      chunksField.put("type", "nested");
      chunksField.put("properties", chunksProperties);

      // Build model-level properties
      Map<String, Object> modelLevelProperties = new HashMap<>();
      modelLevelProperties.put("chunks", chunksField);
      modelLevelProperties.put("totalChunks", ImmutableMap.of("type", "integer"));
      modelLevelProperties.put("modelVersion", ImmutableMap.of("type", "keyword"));
      modelLevelProperties.put("generatedAt", ImmutableMap.of("type", "date"));

      Map<String, Object> modelFieldConfig = ImmutableMap.of("properties", modelLevelProperties);

      modelProperties.put(modelKey, modelFieldConfig);
    }

    // Build embeddings field map
    Map<String, Object> embeddingsField = ImmutableMap.of("properties", modelProperties);

    return embeddingsField;
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
