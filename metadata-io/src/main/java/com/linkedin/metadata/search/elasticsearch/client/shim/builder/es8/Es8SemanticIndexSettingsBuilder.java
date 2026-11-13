package com.linkedin.metadata.search.elasticsearch.client.shim.builder.es8;

import com.linkedin.metadata.utils.elasticsearch.shim.SemanticIndexSpec;
import java.util.LinkedHashMap;
import java.util.Map;
import javax.annotation.Nonnull;

/**
 * Builds the Elasticsearch 8.x index settings for a semantic search index.
 *
 * <p>Returns an empty settings map: ES 8 has no semantic-search-specific index-level toggle (k-NN
 * is built into the {@code dense_vector} field type), and shard/replica counts are left to engine
 * defaults so that operators can override them via cluster-level index templates.
 */
public final class Es8SemanticIndexSettingsBuilder {

  private Es8SemanticIndexSettingsBuilder() {}

  @Nonnull
  public static Map<String, Object> build(@Nonnull SemanticIndexSpec spec) {
    return new LinkedHashMap<>();
  }
}
