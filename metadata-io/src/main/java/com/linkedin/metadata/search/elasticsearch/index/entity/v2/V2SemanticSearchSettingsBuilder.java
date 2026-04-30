package com.linkedin.metadata.search.elasticsearch.index.entity.v2;

import com.linkedin.metadata.config.search.IndexConfiguration;
import com.linkedin.metadata.search.elasticsearch.index.SettingsBuilder;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim.SearchEngineType;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * Settings builder for semantic search indices that extends V2 index settings with k-NN support for
 * vector similarity search.
 *
 * <p>Engine-aware: emits the OpenSearch-only {@code "knn": true} index setting only when the
 * configured search engine is OpenSearch 2. Elasticsearch 8 rejects this setting because k-NN is
 * built into the {@code dense_vector} field type and there is no index-level toggle.
 *
 * <p>Index naming: This builder applies to indices with "_semantic" suffix (e.g.,
 * "datasetindex_v2_semantic").
 */
@Slf4j
public class V2SemanticSearchSettingsBuilder implements SettingsBuilder {
  private final IndexConvention indexConvention;
  private final SettingsBuilder v2SettingsBuilder;
  private final SearchClientShim<?> searchClientShim;

  /**
   * Production constructor — engine type is derived from the injected shim at runtime. Pass {@code
   * null} only from tests that don't care about engine-specific behaviour; production wiring always
   * provides a shim.
   */
  public V2SemanticSearchSettingsBuilder(
      @Nonnull IndexConvention indexConvention,
      @Nonnull SettingsBuilder v2SettingsBuilder,
      @Nullable SearchClientShim<?> searchClientShim) {
    this.indexConvention = indexConvention;
    this.v2SettingsBuilder = v2SettingsBuilder;
    this.searchClientShim = searchClientShim;
  }

  /**
   * Backwards-compatible constructor for callers that don't yet pass a shim (defaults to OS 2
   * behaviour, i.e. emits {@code "knn": true}).
   *
   * @deprecated Inject a {@link SearchClientShim} so the builder produces the correct settings for
   *     the target engine.
   */
  @Deprecated
  public V2SemanticSearchSettingsBuilder(
      @Nonnull IndexConvention indexConvention, @Nonnull SettingsBuilder v2SettingsBuilder) {
    this(indexConvention, v2SettingsBuilder, null);
  }

  /**
   * Builds Elasticsearch settings for a semantic search index by extending V2 settings with k-NN
   * support when the target engine requires it.
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

    if (shouldEnableIndexLevelKnn()) {
      settings.put("knn", true);
      log.debug("Enabled k-NN for index: {}", indexName);
    }

    return settings;
  }

  private boolean shouldEnableIndexLevelKnn() {
    // ES 8 rejects "index.knn" as an unknown setting because dense_vector handles k-NN at the
    // field level. Only OpenSearch 2 (and the legacy null-shim test path) want the toggle.
    if (searchClientShim == null) {
      return true;
    }
    return searchClientShim.getEngineType() != SearchEngineType.ELASTICSEARCH_8;
  }
}
