package com.linkedin.metadata.search.elasticsearch.client.shim.builder.opensearch2;

import com.linkedin.metadata.utils.elasticsearch.shim.SemanticIndexSpec;
import java.util.LinkedHashMap;
import java.util.Map;
import javax.annotation.Nonnull;

/**
 * Builds the OpenSearch 2.x index settings for a semantic search index.
 *
 * <p>OpenSearch requires the top-level {@code "knn": true} index setting to enable the k-NN plugin
 * for an index. This is distinct from Elasticsearch 8, which uses the {@code dense_vector} field
 * mapping and does not require this separate index-level setting.
 *
 * <p>Shard and replica counts are left to engine defaults so that operators can override them via
 * cluster-level index templates.
 */
public final class OpenSearch2SemanticIndexSettingsBuilder {

  private OpenSearch2SemanticIndexSettingsBuilder() {}

  @Nonnull
  public static Map<String, Object> build(@Nonnull SemanticIndexSpec spec) {
    Map<String, Object> settings = new LinkedHashMap<>();
    settings.put("knn", true);
    return settings;
  }
}
