package com.linkedin.metadata.search.elasticsearch.index.entity.v3;

import com.datahub.context.OperationFingerprint;
import com.linkedin.metadata.config.search.EntityIndexConfiguration;
import com.linkedin.metadata.config.search.IndexConfiguration;
import com.linkedin.metadata.config.search.SearchGroupIndexSettingsConfiguration;
import com.linkedin.metadata.config.search.SemanticSearchConfiguration;
import com.linkedin.metadata.search.elasticsearch.index.BaseConfigurationLoader;
import com.linkedin.metadata.search.elasticsearch.index.SettingsBuilder;
import com.linkedin.metadata.search.elasticsearch.index.entity.SemanticEmbeddingFieldConfigBuilder;
import com.linkedin.metadata.search.elasticsearch.index.entity.SemanticEmbeddingMappings;
import com.linkedin.metadata.search.elasticsearch.index.entity.v2.V2LegacySettingsBuilder;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;

/** Builder for generating settings for elasticsearch indices with entity-based field structure */
public class MultiEntitySettingsBuilder implements SettingsBuilder {
  static final String PRIMARY_SEARCH_GROUP = "primary";
  private static final String ANALYSIS_SETTING = "analysis";

  public final Map<String, Object> settings;
  private final Map<String, Object> analyzerConfiguration;
  private final Integer maxFieldsLimit;
  @Nonnull private final SearchGroupIndexSettingsConfiguration defaultSearchGroupSettings;
  @Nonnull private final Map<String, SearchGroupIndexSettingsConfiguration> searchGroupSettings;
  @Nonnull private final IndexConvention indexConvention;
  @Nullable private final SearchClientShim<?> searchClientShim;
  @Nullable private final SemanticSearchConfiguration semanticSearchConfiguration;

  /**
   * Creates a SettingsBuilder with analyzer configuration loaded from a resource path. This
   * constructor should be used when the analyzer configuration path comes from application
   * configuration.
   *
   * @param entityIndexConfiguration the entity index configuration
   * @param indexConvention the index convention for name validation (required)
   * @throws IOException if the configuration resource cannot be read
   */
  public MultiEntitySettingsBuilder(
      @Nonnull EntityIndexConfiguration entityIndexConfiguration,
      @Nonnull IndexConvention indexConvention)
      throws IOException {
    this(entityIndexConfiguration, indexConvention, null);
  }

  /** Semantic configuration defaults to the entity index configuration's own semantic block. */
  public MultiEntitySettingsBuilder(
      @Nonnull EntityIndexConfiguration entityIndexConfiguration,
      @Nonnull IndexConvention indexConvention,
      @Nullable SearchClientShim<?> searchClientShim)
      throws IOException {
    this(
        entityIndexConfiguration,
        indexConvention,
        searchClientShim,
        entityIndexConfiguration.getSemanticSearch());
  }

  public MultiEntitySettingsBuilder(
      @Nonnull EntityIndexConfiguration entityIndexConfiguration,
      @Nonnull IndexConvention indexConvention,
      @Nullable SearchClientShim<?> searchClientShim,
      @Nullable SemanticSearchConfiguration semanticSearchConfiguration)
      throws IOException {
    this.indexConvention = indexConvention;
    this.searchClientShim = searchClientShim;
    this.semanticSearchConfiguration = semanticSearchConfiguration;
    this.maxFieldsLimit = entityIndexConfiguration.getV3().getMaxFieldsLimit();
    this.defaultSearchGroupSettings =
        Optional.ofNullable(entityIndexConfiguration.getV3().getDefaultSettings())
            .orElseGet(SearchGroupIndexSettingsConfiguration::new);
    this.searchGroupSettings =
        Optional.ofNullable(entityIndexConfiguration.getV3().getGroups()).orElseGet(Map::of);

    if (!entityIndexConfiguration.getV3().getAnalyzerConfig().trim().isEmpty()) {
      this.analyzerConfiguration =
          loadAnalyzerConfigurationFromResource(
              entityIndexConfiguration.getV3().getAnalyzerConfig());
    } else {
      this.analyzerConfiguration = null;
    }

    settings = buildBaseSettings();
  }

  @Override
  public Map<String, Object> getSettings(
      @Nonnull IndexConfiguration indexConfiguration, @Nonnull String indexName) {
    // For v3, only apply settings to indices that match the v3 entity naming pattern. Prefix-
    // INDEPENDENT type check: the index name is already fully resolved (may carry a per-operation
    // prefix); this bootstrap path only needs its type, not a prefix-scoped match.
    if (!indexConvention.isV3EntityIndexType(indexName)) {
      return new HashMap<>();
    }
    Map<String, Object> indexSettings = buildIndexSettings(indexConfiguration);
    applySearchGroupSettings(indexSettings, indexName);
    // The shared mapping includes embeddings whenever semantic indexing is enabled, including
    // when no entity has opted in. OpenSearch rejects knn_vector unless index.knn is true on
    // every index that receives that mapping. Elasticsearch rejects index.knn.
    if (shouldEnableIndexLevelKnn(indexName)
        || (SemanticEmbeddingFieldConfigBuilder.semanticIndexingEnabled(semanticSearchConfiguration)
            && SemanticEmbeddingMappings.shouldEnableIndexLevelKnn(searchClientShim))) {
      indexSettings.put("knn", true);
    }
    return indexSettings;
  }

  /**
   * Loads analyzer configuration from a resource. Supports both JSON and YAML formats based on
   * resource extension.
   *
   * @param resourcePath resource path to the configuration
   * @return Map containing the analyzer configuration
   * @throws IOException if the resource cannot be read
   */
  private static Map<String, Object> loadAnalyzerConfigurationFromResource(String resourcePath)
      throws IOException {
    Map<String, Object> config =
        BaseConfigurationLoader.loadConfigurationFromResource(resourcePath);
    return BaseConfigurationLoader.extractAnalysisSection(config, resourcePath);
  }

  /**
   * Builds base settings from explicit V3 analyzer configuration. The V3 analyzers reuse V2 filters
   * (synonyms, stop words, stem overrides), and legacy browse paths use V2's path analyzers; the V2
   * analysis is merged in getSettings where IndexConfiguration is available.
   */
  private Map<String, Object> buildBaseSettings() {
    Map<String, Object> baseSettings = new HashMap<>();

    // Add analysis configuration if available
    if (analyzerConfiguration != null) {
      baseSettings.put(ANALYSIS_SETTING, analyzerConfiguration);
    }

    // Set field limit for v3 indices to handle many aspects and fields, regardless of analyzers.
    if (maxFieldsLimit != null) {
      baseSettings.put("mapping.total_fields.limit", maxFieldsLimit);
    }

    return baseSettings;
  }

  private Map<String, Object> buildIndexSettings(
      @Nonnull final IndexConfiguration indexConfiguration) {
    Map<String, Object> indexSettings = new HashMap<>(settings);
    try {
      indexSettings.put(ANALYSIS_SETTING, buildMergedAnalysisConfiguration(indexConfiguration));
      indexSettings.put(V2LegacySettingsBuilder.MAX_NGRAM_DIFF, 17);
    } catch (IOException e) {
      throw new RuntimeException("Failed to build V3 analyzer settings", e);
    }
    return indexSettings;
  }

  private Map<String, Object> buildMergedAnalysisConfiguration(
      @Nonnull final IndexConfiguration indexConfiguration) throws IOException {
    // Reuse the V2 analysis files shipped in this build. The v2.5 stem and synonym files are not
    // here; a build that has them can pass enableSearchV2_5.
    Map<String, Object> legacyAnalysis =
        new V2LegacySettingsBuilder(indexConfiguration, indexConvention, false)
            .buildAnalysisSettings(indexConfiguration, false);
    if (analyzerConfiguration == null || analyzerConfiguration.isEmpty()) {
      return withRichTextAnalyzers(new HashMap<>(legacyAnalysis));
    }
    return withRichTextAnalyzers(
        withMainTokenizer(
            mergeAnalysisSections(legacyAnalysis, analyzerConfiguration),
            indexConfiguration.getMainTokenizer()));
  }

  /**
   * Analyzers for a V3 {@code RICH_TEXT} source field. V2 indices do not define them; a V2 mapping
   * of that type uses the existing text field instead.
   */
  @SuppressWarnings("unchecked")
  private static Map<String, Object> withRichTextAnalyzers(
      @Nonnull final Map<String, Object> analysis) {
    final Map<String, Object> filters = new HashMap<>();
    if (analysis.get(V2LegacySettingsBuilder.FILTER) instanceof Map<?, ?> existing) {
      filters.putAll((Map<String, Object>) existing);
    }
    filters.put(
        FieldTypeMapper.RICH_TEXT_DELIMITER,
        Map.of(
            "type",
            V2LegacySettingsBuilder.WORD_DELIMITER_GRAPH,
            V2LegacySettingsBuilder.SPLIT_ON_NUMERICS,
            true,
            V2LegacySettingsBuilder.SPLIT_ON_CASE_CHANGE,
            true,
            V2LegacySettingsBuilder.PRESERVE_ORIGINAL,
            true));

    final Map<String, Object> analyzers = new HashMap<>();
    if (analysis.get(V2LegacySettingsBuilder.ANALYZER) instanceof Map<?, ?> existing) {
      analyzers.putAll((Map<String, Object>) existing);
    }
    analyzers.put(
        FieldTypeMapper.RICH_TEXT_ANALYZER,
        Map.of(
            V2LegacySettingsBuilder.TOKENIZER,
            "standard",
            V2LegacySettingsBuilder.FILTER,
            List.of(
                FieldTypeMapper.RICH_TEXT_DELIMITER,
                V2LegacySettingsBuilder.FLATTEN_GRAPH,
                V2LegacySettingsBuilder.LOWERCASE,
                V2LegacySettingsBuilder.ASCII_FOLDING,
                V2LegacySettingsBuilder.SNOWBALL)));
    analyzers.put(
        FieldTypeMapper.RICH_TEXT_SEARCH_ANALYZER,
        Map.of(
            V2LegacySettingsBuilder.TOKENIZER,
            "standard",
            V2LegacySettingsBuilder.FILTER,
            List.of(
                FieldTypeMapper.RICH_TEXT_DELIMITER,
                V2LegacySettingsBuilder.LOWERCASE,
                V2LegacySettingsBuilder.ASCII_FOLDING,
                V2LegacySettingsBuilder.SNOWBALL)));

    final Map<String, Object> result = new HashMap<>(analysis);
    result.put(V2LegacySettingsBuilder.FILTER, filters);
    result.put(V2LegacySettingsBuilder.ANALYZER, analyzers);
    return result;
  }

  /**
   * A configured main tokenizer (ELASTICSEARCH_MAIN_TOKENIZER, such as a language plugin's)
   * replaces the word tokenizer of the shared search fields' analyzers, as it replaces V2's.
   */
  @SuppressWarnings("unchecked")
  private static Map<String, Object> withMainTokenizer(
      @Nonnull final Map<String, Object> analysis, @Nullable final String mainTokenizer) {
    if (StringUtils.isBlank(mainTokenizer)
        || !(analysis.get("analyzer") instanceof Map<?, ?> analyzers)) {
      return analysis;
    }
    final Map<String, Object> withTokenizer = new HashMap<>();
    ((Map<String, Object>) analyzers)
        .forEach(
            (name, analyzer) -> {
              if (analyzer instanceof Map<?, ?> definition
                  && V3SearchFields.WORD_TOKENIZER.equals(definition.get("tokenizer"))) {
                final Map<String, Object> replaced =
                    new HashMap<>((Map<String, Object>) definition);
                replaced.put("tokenizer", mainTokenizer);
                withTokenizer.put(name, replaced);
              } else {
                withTokenizer.put(name, analyzer);
              }
            });
    final Map<String, Object> result = new HashMap<>(analysis);
    result.put("analyzer", withTokenizer);
    return result;
  }

  @SuppressWarnings("unchecked")
  private static Map<String, Object> mergeAnalysisSections(
      @Nonnull final Map<String, Object> baseAnalysis,
      @Nonnull final Map<String, Object> overrideAnalysis) {
    Map<String, Object> merged = new HashMap<>(baseAnalysis);
    overrideAnalysis.forEach(
        (sectionName, overrideValue) -> {
          Object baseValue = merged.get(sectionName);
          if (baseValue instanceof Map && overrideValue instanceof Map) {
            Map<String, Object> section = new HashMap<>((Map<String, Object>) baseValue);
            section.putAll((Map<String, Object>) overrideValue);
            merged.put(sectionName, section);
          } else {
            merged.put(sectionName, overrideValue);
          }
        });
    return merged;
  }

  private boolean shouldEnableIndexLevelKnn(@Nonnull final String indexName) {
    // Per-entity V3 indices (e.g. documentindex_v3) carry root embeddings when semantic search is
    // enabled for that entity — those need index.knn independently of the primary search group.
    if (SemanticEmbeddingMappings.isSemanticEnabledV3Index(
        semanticSearchConfiguration, indexName)) {
      return SemanticEmbeddingMappings.shouldEnableIndexLevelKnn(searchClientShim);
    }
    // Index settings/mappings are built at index-creation time — no request context;
    // resolves the deploy-wide prefix.
    if (!indexName.equals(
        indexConvention.getEntityIndexNameV3(OperationFingerprint.EMPTY, PRIMARY_SEARCH_GROUP))) {
      return false;
    }
    // Same gate as the embedding mappings: without vector fields, index.knn only adds
    // native-memory circuit-breaker overhead.
    if (!SemanticEmbeddingFieldConfigBuilder.semanticIndexingEnabled(semanticSearchConfiguration)) {
      return false;
    }
    return SemanticEmbeddingMappings.shouldEnableIndexLevelKnn(searchClientShim);
  }

  private void applySearchGroupSettings(
      @Nonnull final Map<String, Object> indexSettings, @Nonnull final String indexName) {
    final SearchGroupIndexSettingsConfiguration settingsConfiguration =
        resolveSearchGroupSettings(indexName);
    if (settingsConfiguration.getNumberOfShards() != null) {
      indexSettings.put("number_of_shards", settingsConfiguration.getNumberOfShards());
    }
    if (settingsConfiguration.getNumberOfReplicas() != null) {
      indexSettings.put("number_of_replicas", settingsConfiguration.getNumberOfReplicas());
    }
  }

  @Nonnull
  private SearchGroupIndexSettingsConfiguration resolveSearchGroupSettings(
      @Nonnull final String indexName) {
    // Index settings/mappings are built at index-creation time — no request context;
    // resolves the deploy-wide prefix.
    return searchGroupSettings.entrySet().stream()
        .filter(
            entry ->
                indexName.equals(
                    indexConvention.getEntityIndexNameV3(
                        OperationFingerprint.EMPTY, entry.getKey())))
        .map(Map.Entry::getValue)
        .filter(settingsConfiguration -> settingsConfiguration != null)
        .findFirst()
        .orElse(defaultSearchGroupSettings);
  }
}
