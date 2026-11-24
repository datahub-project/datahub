package com.linkedin.metadata.search.embedding;

import java.util.Locale;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * A named embedding. The id is the {@code SemanticContent} embeddings map key the vectors are
 * stored under, so a profile has no second name and adopting it needs no reindex.
 *
 * @param id embeddings map key
 * @param profileClass representation class, such as {@code textChunks}
 * @param modelId query embedding model; the provider's model id carries its version
 * @param metric similarity metric of the stored vectors, as configured ({@code cosinesimil}, {@code
 *     cosine}, {@code l2}, or an inner product spelling)
 * @param entityTypes entity types the profile covers. A type outside the set is not covered.
 * @param systems search systems the profile is available on: {@code elasticsearch}, {@code
 *     opensearch}, or a configured external store name
 */
public record EmbeddingProfile(
    @Nonnull String id,
    @Nonnull String profileClass,
    @Nullable String modelId,
    @Nonnull String metric,
    @Nonnull Set<String> entityTypes,
    @Nonnull Set<String> systems) {

  public static final String TEXT_CHUNKS = "textChunks";

  private static final Set<String> COSINE_METRICS = Set.of("cosine", "cosinesimil");

  public EmbeddingProfile {
    entityTypes = Set.copyOf(entityTypes);
    systems = Set.copyOf(systems);
  }

  public boolean covers(@Nonnull String entityType) {
    return entityTypes.contains(entityType);
  }

  public boolean availableOn(@Nullable String system) {
    return system != null && systems.contains(system);
  }

  /** True when a raw cosine floor ({@code retrieval.minSimilarity}) means something here. */
  public boolean cosine() {
    return COSINE_METRICS.contains(metric.trim().toLowerCase(Locale.ROOT));
  }
}
