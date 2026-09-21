package com.linkedin.metadata.search.elasticsearch.index.entity.v2;

import com.google.common.collect.ImmutableMap;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.config.search.SemanticSearchConfiguration;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.search.elasticsearch.index.MappingsBuilder;
import com.linkedin.metadata.search.elasticsearch.index.entity.SemanticEmbeddingMappings;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
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

  private Map<String, Object> buildEmbeddingFieldConfig() {
    return SemanticEmbeddingMappings.buildEmbeddingFieldConfig(semanticConfig, searchClientShim);
  }

  /**
   * Transforms base V2 index mappings into semantic search index mappings by adding embeddings
   * field.
   *
   * @param baseIndexMappings Base V2 index mappings to transform
   * @return Semantic search index mappings with embeddings field added
   */
  private Collection<IndexMapping> addSemanticMappings(
      @Nonnull OperationContext opContext, Collection<IndexMapping> baseIndexMappings) {
    Set<String> enabledEntities = semanticConfig.getEnabledEntities();
    Map<String, Object> embeddingFieldConfig = buildEmbeddingFieldConfig();
    ArrayList<IndexMapping> semanticIndexMappings = new ArrayList<>();

    for (IndexMapping baseIndexMapping : baseIndexMappings) {
      String indexName = baseIndexMapping.getIndexName();
      String entityName = indexConvention.getEntityName(opContext, indexName).orElse(null);

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
      newPropertiesMap.putAll(SemanticEmbeddingMappings.provenanceRootMappings());

      // Construct new top-level map with new properties map
      ImmutableMap.Builder<String, Object> newMappings = new ImmutableMap.Builder<>();
      newMappings.putAll(baseMappings);
      newMappings.put("properties", newPropertiesMap.build());

      // Construct new IndexMapping object for semantic search
      Map<String, Object> finalMappings = newMappings.buildKeepingLast();

      String semanticIndexName = indexConvention.getEntityIndexNameSemantic(opContext, entityName);
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
    return addSemanticMappings(opContext, baseIndexMappings);
  }

  @Override
  public Collection<IndexMapping> getIndexMappingsWithNewStructuredProperty(
      @Nonnull OperationContext opContext,
      @Nonnull Urn urn,
      @Nonnull StructuredPropertyDefinition property) {
    Collection<IndexMapping> baseIndexMappings =
        v2MappingsBuilder.getIndexMappingsWithNewStructuredProperty(opContext, urn, property);
    return addSemanticMappings(opContext, baseIndexMappings);
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
