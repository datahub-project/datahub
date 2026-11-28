package com.linkedin.metadata.search.semantic.profile;

import com.linkedin.metadata.config.search.ModelEmbeddingConfig;
import com.linkedin.metadata.config.search.SemanticSearchConfiguration;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.V3IndexKeys;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.IntStream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * The one rule for which embedding vectors Search V3 accepts. The SQL write validator and the index
 * projector both ask it, so a vector either reaches SQL and the index or reaches neither. Removal
 * is not gated: callers keep a null profile and let it through.
 *
 * <p>A profile is accepted when its entity type is eligible, its model is configured, and every
 * chunk vector has the configured dimension. An entity type is eligible when it is in the primary
 * search group, whose index always maps embeddings, or when semantic search is enabled for it.
 */
public final class SemanticVectorGate {

  /** The search group whose index maps embeddings for every entity in it. */
  public static final String PRIMARY_SEARCH_GROUP = "primary";

  /** Why a profile is not accepted. Checked in this order. */
  public enum Rejection {
    ENTITY_NOT_ENABLED,
    MODEL_NOT_CONFIGURED,
    DIMENSION_MISMATCH
  }

  private final Set<String> enabledEntities;
  private final Map<String, Integer> modelDimensions;

  public SemanticVectorGate(
      @Nonnull Set<String> enabledEntities, @Nonnull Map<String, Integer> modelDimensions) {
    this.enabledEntities = Set.copyOf(enabledEntities);
    this.modelDimensions = Map.copyOf(modelDimensions);
  }

  /** A gate that accepts no vectors, which is what a missing semantic search block means. */
  @Nonnull
  public static SemanticVectorGate from(@Nullable SemanticSearchConfiguration configuration) {
    if (configuration == null) {
      return new SemanticVectorGate(Set.of(), Map.of());
    }
    Map<String, Integer> dimensions = new LinkedHashMap<>();
    if (configuration.getModels() != null) {
      for (Map.Entry<String, ModelEmbeddingConfig> model : configuration.getModels().entrySet()) {
        if (model.getValue() != null) {
          dimensions.put(model.getKey(), model.getValue().getVectorDimension());
        }
      }
    }
    Set<String> entities =
        configuration.isEnabled() && configuration.getEnabledEntities() != null
            ? Set.copyOf(configuration.getEnabledEntities())
            : Set.of();
    return new SemanticVectorGate(entities, dimensions);
  }

  public boolean isEntityEligible(@Nonnull EntitySpec entitySpec) {
    return PRIMARY_SEARCH_GROUP.equals(V3IndexKeys.resolve(entitySpec))
        || enabledEntities.contains(entitySpec.getName());
  }

  public boolean isModelConfigured(@Nonnull String modelKey) {
    return modelDimensions.containsKey(modelKey);
  }

  /** Configured dimension per model key, which is also the set of live models. */
  @Nonnull
  public Map<String, Integer> modelDimensions() {
    return modelDimensions;
  }

  public boolean isValidDimension(@Nonnull String modelKey, int vectorSize) {
    Integer expected = modelDimensions.get(modelKey);
    return expected != null && expected > 0 && vectorSize == expected;
  }

  /**
   * First reason the profile is not accepted, empty when it is. {@code vectorSizes} holds one size
   * per chunk, with 0 for a chunk that has no vector.
   */
  @Nonnull
  public Optional<Rejection> reject(
      @Nonnull EntitySpec entitySpec, @Nonnull String modelKey, @Nonnull IntStream vectorSizes) {
    if (!isEntityEligible(entitySpec)) {
      return Optional.of(Rejection.ENTITY_NOT_ENABLED);
    }
    if (!isModelConfigured(modelKey)) {
      return Optional.of(Rejection.MODEL_NOT_CONFIGURED);
    }
    if (vectorSizes.anyMatch(size -> !isValidDimension(modelKey, size))) {
      return Optional.of(Rejection.DIMENSION_MISMATCH);
    }
    return Optional.empty();
  }
}
