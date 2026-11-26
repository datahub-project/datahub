package com.linkedin.metadata.search.elasticsearch.index.entity.v3;

import com.linkedin.metadata.config.search.SemanticSearchConfiguration;
import com.linkedin.metadata.search.elasticsearch.index.entity.SemanticEmbeddingMappings;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Adds root {@code embeddings} and provenance fields to every V3 index when semantic search is
 * enabled for any entity. One mapping document is applied to every index, so the vector field is
 * present on indices whose entities do not store vectors. Which entity types are embedded is
 * decided when documents are written and when a query chooses its indices.
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
    // indexKey is unused: the same embeddings object is applied to every V3 index.
    if (!SemanticEmbeddingMappings.enabledForAnyEntity(semanticConfig)) {
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
