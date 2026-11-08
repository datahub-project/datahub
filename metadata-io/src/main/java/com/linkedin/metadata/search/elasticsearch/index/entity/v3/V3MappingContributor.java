package com.linkedin.metadata.search.elasticsearch.index.entity.v3;

import java.util.Map;
import javax.annotation.Nonnull;

/**
 * Additive hook for extra root-level V3 mapping {@code properties}. OSS ships no implementations.
 * Keys are field names; values are Elasticsearch mapping definitions.
 *
 * <p>Keys that already exist in the generated mapping are rejected; contributors cannot overwrite
 * strategy-owned fields.
 */
public interface V3MappingContributor {

  @Nonnull
  Map<String, Object> extraRootProperties();

  /**
   * Extra root properties for a specific V3 index key (entity-named search group, e.g. {@code
   * document} vs {@code dataset}). Default delegates to {@link #extraRootProperties()}.
   */
  @Nonnull
  default Map<String, Object> extraRootProperties(@Nonnull String indexKey) {
    return extraRootProperties();
  }
}
