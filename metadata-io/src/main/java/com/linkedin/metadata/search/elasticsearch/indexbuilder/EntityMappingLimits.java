package com.linkedin.metadata.search.elasticsearch.indexbuilder;

import java.util.Map;
import javax.annotation.Nonnull;

/**
 * Resolved per-index mapping limit overrides from {@code elasticsearch.index.entityMappingLimits}.
 * The factory translates configured limit keys (e.g. {@code total_fields}) into ES setting paths
 * (e.g. {@code mapping.total_fields.limit}) and entity names into full index names, so this type
 * holds only ready-to-apply pairs.
 *
 * <p>{@link #byIndex} is keyed by the full index name and contains the explicit per-entity values.
 * {@link #defaults} is the fallback applied to any entity index that does not have an explicit
 * entry. Both maps may be empty.
 */
public record EntityMappingLimits(
    @Nonnull Map<String, Map<String, String>> byIndex, @Nonnull Map<String, String> defaults) {

  public static final EntityMappingLimits EMPTY = new EntityMappingLimits(Map.of(), Map.of());

  /**
   * Returns the limits that apply to the given index: the explicit entry if present, otherwise the
   * defaults, otherwise an empty map.
   */
  @Nonnull
  public Map<String, String> forIndex(@Nonnull String indexName) {
    return byIndex.getOrDefault(indexName, defaults);
  }

  public boolean isEmpty() {
    return byIndex.isEmpty() && defaults.isEmpty();
  }
}
