package com.linkedin.metadata.search.hybrid;

import static com.linkedin.metadata.utils.SearchUtil.INDEX_VIRTUAL_FIELD;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.config.search.SemanticSearchConfiguration;
import com.linkedin.metadata.search.elasticsearch.index.entity.SemanticEmbeddingMappings;
import com.linkedin.metadata.search.semantic.SemanticEntitySearchService;
import com.linkedin.metadata.utils.elasticsearch.shim.KnnSearchRequest;
import io.datahubproject.metadata.context.OperationContext;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Builds the kNN leg of hybrid search: a V3-native kNN request over the Search V3 indices that hold
 * vectors for the requested entity types. Under the entity-named V3 layout those are the indices of
 * semantic-enabled entity types, {@code documentindex_v3} by default, while keyword search keeps
 * reading every requested entity index.
 */
public class V3HybridKnnRequestBuilder {

  private static final String CHUNKS_SUFFIX = ".chunks";
  private static final String VECTOR_SUFFIX = ".vector";

  /** Keeps num_candidates, 10 x k by default, within the engines' limit of 10,000. */
  static final int MAX_K = 1_000;

  @Nullable private final SemanticSearchConfiguration semanticSearchConfiguration;

  public V3HybridKnnRequestBuilder(
      @Nullable final SemanticSearchConfiguration semanticSearchConfiguration) {
    this.semanticSearchConfiguration = semanticSearchConfiguration;
  }

  /** Canonical names of the requested entity types whose vectors the kNN leg searches. */
  @Nonnull
  public Set<String> vectorEntityNames(
      @Nonnull OperationContext opContext, @Nonnull final Collection<String> entityNames) {
    return SemanticEntitySearchService.v3SemanticIndices(
            opContext, entityNames, semanticSearchConfiguration)
        .keySet();
  }

  @Nonnull
  public Optional<KnnSearchRequest> build(
      @Nonnull OperationContext opContext,
      @Nonnull final Collection<String> entityNames,
      @Nonnull final String modelEmbeddingKey,
      @Nonnull final float[] queryVector,
      @Nonnull final Collection<Urn> urns,
      @Nonnull final Collection<String> fieldsToFetch,
      @Nullable final Map<String, Object> rootFilter) {
    final Map<String, String> indices =
        SemanticEntitySearchService.v3SemanticIndices(
            opContext, entityNames, semanticSearchConfiguration);
    if (indices.isEmpty() || urns.isEmpty()) {
      return Optional.empty();
    }
    // Only the given rows are scored, so the search is exact and repeatable and every row with a
    // vector gets its score
    final List<String> urnValues = urns.stream().map(Urn::toString).collect(Collectors.toList());

    return Optional.of(
        KnnSearchRequest.builder()
            .indexName(String.join(",", indices.values()))
            .vectorField(vectorField(modelEmbeddingKey))
            .queryVector(queryVector)
            .k(Math.min(urnValues.size(), MAX_K))
            .fieldsToFetch(new ArrayList<>(fieldsToFetch))
            .filter(
                combineFilters(
                    rootFilter,
                    entityTypeFilter(indices.keySet()),
                    Map.of("terms", Map.of("urn", urnValues))))
            .build());
  }

  @Nonnull
  public static String vectorField(@Nonnull final String modelEmbeddingKey) {
    if (modelEmbeddingKey.isBlank()) {
      throw new IllegalArgumentException("modelEmbeddingKey must not be blank");
    }
    return SemanticEmbeddingMappings.EMBEDDINGS_FIELD
        + "."
        + modelEmbeddingKey
        + CHUNKS_SUFFIX
        + VECTOR_SUFFIX;
  }

  @Nonnull
  private static Map<String, Object> entityTypeFilter(
      @Nonnull final Collection<String> entityNames) {
    return Map.of("terms", Map.of(INDEX_VIRTUAL_FIELD, new ArrayList<>(entityNames)));
  }

  @Nonnull
  private static Map<String, Object> combineFilters(
      @Nullable final Map<String, Object> rootFilter,
      @Nonnull final Map<String, Object> entityTypeFilter,
      @Nonnull final Map<String, Object> urnFilter) {
    final List<Map<String, Object>> filters = new ArrayList<>();
    if (rootFilter != null && !rootFilter.isEmpty()) {
      filters.add(rootFilter);
    }
    filters.add(entityTypeFilter);
    filters.add(urnFilter);
    final Map<String, Object> bool = new LinkedHashMap<>();
    bool.put("filter", filters);
    return Map.of("bool", bool);
  }
}
