package com.linkedin.metadata.search.elasticsearch.index.entity.v2;

import com.linkedin.metadata.config.search.IndexConfiguration;
import com.linkedin.metadata.search.elasticsearch.index.SettingsBuilder;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

/**
 * Settings builder for semantic search indices that extends V2 index settings with k-NN support for
 * vector similarity search.
 *
 * <p>Index naming: This builder applies to indices with "_semantic" suffix (e.g.,
 * "datasetindex_v2_semantic").
 */
@Slf4j
public class V2SemanticSearchSettingsBuilder implements SettingsBuilder {
  private final IndexConvention indexConvention;
  private final SettingsBuilder v2SettingsBuilder;

  /**
   * Constructs a new semantic search settings builder.
   *
   * @param v2SettingsBuilder the base V2 settings builder to decorate with k-NN support
   */
  public V2SemanticSearchSettingsBuilder(
      @Nonnull IndexConvention indexConvention, @Nonnull SettingsBuilder v2SettingsBuilder) {
    this.indexConvention = indexConvention;
    this.v2SettingsBuilder = v2SettingsBuilder;
  }

  /**
   * Builds Elasticsearch settings for a semantic search index by extending V2 settings with k-NN
   * support.
   */
  @Override
  public Map<String, Object> getSettings(
      @Nonnull IndexConfiguration indexConfiguration, @Nonnull String indexName) {

    // Only apply k-NN settings to semantic search indices
    if (!indexConvention.isSemanticEntityIndex(indexName)) {
      log.debug("Index '{}' is not a semantic index, returning empty settings", indexName);
      return Map.of();
    }

    Optional<String> entityName = indexConvention.getEntityNameSemantic(indexName);
    if (entityName.isEmpty()) {
      return Map.of();
    }

    String v2IndexName = indexConvention.getEntityIndexName(entityName.get());
    Map<String, Object> settings =
        new HashMap<>(v2SettingsBuilder.getSettings(indexConfiguration, v2IndexName));
    settings.put("knn", true);

    log.debug("Enabled k-NN for index: {}", indexName);
    return settings;
  }
}
