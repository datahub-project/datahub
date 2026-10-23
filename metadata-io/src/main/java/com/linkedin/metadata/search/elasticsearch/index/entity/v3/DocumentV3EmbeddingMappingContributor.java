package com.linkedin.metadata.search.elasticsearch.index.entity.v3;

import com.linkedin.metadata.config.search.SemanticSearchConfiguration;
import com.linkedin.metadata.search.elasticsearch.index.entity.SemanticEmbeddingMappings;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Adds root {@code embeddings} (and provenance keyword/date fields) to document V3 mappings when
 * semantic search is enabled for that entity. Other V3 indices are unchanged.
 */
public class DocumentV3EmbeddingMappingContributor implements V3MappingContributor {

  private final SemanticSearchConfiguration semanticConfig;
  private final SearchClientShim<?> searchClientShim;

  public DocumentV3EmbeddingMappingContributor(
      @Nullable SemanticSearchConfiguration semanticConfig,
      @Nullable SearchClientShim<?> searchClientShim) {
    this.semanticConfig = semanticConfig;
    this.searchClientShim = searchClientShim;
  }

  @Nonnull
  @Override
  public Map<String, Object> extraRootProperties() {
    return Map.of();
  }

  @Nonnull
  @Override
  public Map<String, Object> extraRootProperties(@Nonnull String indexKey) {
    if (!SemanticEmbeddingMappings.isEnabledForEntity(semanticConfig, indexKey)) {
      return Map.of();
    }
    Map<String, Object> extras = new HashMap<>();
    extras.put(
        SemanticEmbeddingMappings.EMBEDDINGS_FIELD,
        SemanticEmbeddingMappings.buildEmbeddingFieldConfig(semanticConfig, searchClientShim));
    extras.putAll(SemanticEmbeddingMappings.provenanceRootMappings());
    return extras;
  }
}
