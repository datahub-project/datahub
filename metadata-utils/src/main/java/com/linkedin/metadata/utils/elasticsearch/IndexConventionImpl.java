package com.linkedin.metadata.utils.elasticsearch;

import static com.linkedin.metadata.Constants.DATAHUB_USAGE_EVENT_INDEX;
import static com.linkedin.metadata.Constants.GRAPH_SERVICE_INDEX;
import static com.linkedin.metadata.Constants.SCHEMA_FIELD_ENTITY_NAME;
import static com.linkedin.metadata.Constants.SYSTEM_METADATA_SERVICE_INDEX;

import com.datahub.context.OperationFingerprint;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.config.search.EntityIndexConfiguration;
import com.linkedin.metadata.config.search.SearchClusterSettings;
import com.linkedin.metadata.config.search.SearchComponent;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.utils.SchemaFieldUtils;
import com.linkedin.util.Pair;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Builder;
import lombok.Getter;
import lombok.Value;
import org.apache.commons.lang3.StringUtils;

// Default implementation of search index naming convention
public class IndexConventionImpl implements IndexConvention {

  /** A convention with no prefix under any operation (tests / EMPTY search context). */
  public static IndexConvention noPrefix(
      @Nonnull String idHashAlgo, @Nonnull EntityIndexConfiguration entityIndexConfiguration) {
    return new IndexConventionImpl(
        IndexConventionConfig.builder().hashIdAlgo(idHashAlgo).build(),
        new ConfiguredIndexPrefixResolver(""),
        entityIndexConfiguration);
  }

  // Bounded cache of resolved index names, keyed by (prefix, base name). The prefix varies per
  // operation, so it is part of the key — otherwise a per-operation prefix would be memoized
  // against
  // the wrong base name and leak across operations. The bound matters because this is a singleton
  // bean: a multi-prefix (e.g. per-namespace) deployment would otherwise grow the map without limit
  // (prefixes × base names). A miss just recomputes the (cheap) name.
  private static final int INDEX_NAME_CACHE_MAX_SIZE = 10_000;

  // Cleared wholesale once it exceeds the bound (see getIndexName) rather than per-entry LRU, so
  // cache hits stay lock-free on a ConcurrentHashMap instead of contending on a single monitor.
  private final Map<String, String> indexNameMapping = new ConcurrentHashMap<>();
  private final IndexPrefixResolver prefixResolver;

  @Getter private final IndexConventionConfig indexConventionConfig;

  private final EntityIndexConfiguration entityIndexConfiguration;

  private static final String ENTITY_INDEX_VERSION = "v2";
  private static final String ENTITY_INDEX_VERSION_V3 = "v3";
  private static final String ENTITY_INDEX_SUFFIX = "index";
  private static final String SEMANTIC_INDEX_SUFFIX = "semantic";

  private final Map<SearchComponent, String> componentPrefixOverlays;

  public IndexConventionImpl(
      IndexConventionConfig indexConventionConfig,
      @Nonnull IndexPrefixResolver prefixResolver,
      @Nonnull EntityIndexConfiguration entityIndexConfiguration) {
    this(indexConventionConfig, prefixResolver, entityIndexConfiguration, Map.of());
  }

  public IndexConventionImpl(
      IndexConventionConfig indexConventionConfig,
      @Nonnull IndexPrefixResolver prefixResolver,
      @Nonnull EntityIndexConfiguration entityIndexConfiguration,
      @Nullable Map<SearchComponent, String> componentPrefixOverlays) {
    this.indexConventionConfig = indexConventionConfig;
    this.prefixResolver = prefixResolver;
    this.entityIndexConfiguration = entityIndexConfiguration;
    this.componentPrefixOverlays = copyOverlays(componentPrefixOverlays);
  }

  /**
   * Explicit per-component index prefixes from {@code elasticsearch.clusters.*.index.prefix}. Blank
   * overlays inherit {@link IndexPrefixResolver} (deploy-wide {@code INDEX_PREFIX} or a Cloud
   * per-operation resolver).
   */
  @Nonnull
  public static Map<SearchComponent, String> explicitComponentPrefixOverlays(
      @Nullable ElasticSearchConfiguration esConfig) {
    if (esConfig == null) {
      return Map.of();
    }
    Map<String, SearchClusterSettings> clusters = esConfig.getClusters();
    if (clusters == null || clusters.isEmpty()) {
      return Map.of();
    }
    EnumMap<SearchComponent, String> overlays = new EnumMap<>(SearchComponent.class);
    for (SearchComponent component : SearchComponent.values()) {
      SearchClusterSettings cluster =
          clusters.get(esConfig.getComponentCluster().clusterFor(component));
      if (cluster == null || cluster.getIndex() == null) {
        continue;
      }
      String overlay = SearchClusterSettings.blankToNull(cluster.getIndex().getPrefix());
      if (overlay != null) {
        overlays.put(component, overlay);
      }
    }
    return overlays.isEmpty() ? Map.of() : Map.copyOf(overlays);
  }

  @Nonnull
  private static Map<SearchComponent, String> copyOverlays(
      @Nullable Map<SearchComponent, String> overlays) {
    if (overlays == null || overlays.isEmpty()) {
      return Map.of();
    }
    EnumMap<SearchComponent, String> copy = new EnumMap<>(SearchComponent.class);
    for (Map.Entry<SearchComponent, String> entry : overlays.entrySet()) {
      String value = SearchClusterSettings.blankToNull(entry.getValue());
      if (value != null) {
        copy.put(entry.getKey(), value);
      }
    }
    return copy.isEmpty() ? Map.of() : Map.copyOf(copy);
  }

  /** The resolver prefix for {@code operation}, empty string meaning "no prefix". */
  private String prefix(@Nonnull OperationFingerprint operation) {
    String resolved = prefixResolver.resolvePrefix(operation);
    // Canonicalize to lower case (Locale.ROOT) so index names, cleanup/search patterns, and inverse
    // parsing all agree: index names are lower-cased at creation, so the prefix must be too.
    // Without
    // this, a resolver returning "Acme" would build "acme_datasetindex_v2" but produce "Acme_*"
    // patterns and expect an "Acme_" prefix when parsing.
    return StringUtils.isEmpty(resolved) ? "" : resolved.toLowerCase(Locale.ROOT);
  }

  /**
   * Prefix for this family: a non-blank cluster overlay replaces {@code INDEX_PREFIX}; otherwise
   * the operation's {@link IndexPrefixResolver} value.
   */
  private String prefix(
      @Nonnull OperationFingerprint operation, @Nullable SearchComponent component) {
    if (component != null) {
      String overlay = componentPrefixOverlays.get(component);
      if (overlay != null) {
        return overlay.toLowerCase(Locale.ROOT);
      }
    }
    return prefix(operation);
  }

  /** The prefix token spliced ahead of a base name, e.g. {@code "prod_"} or {@code ""}. */
  private String prefixToken(
      @Nonnull OperationFingerprint operation, @Nullable SearchComponent component) {
    String prefix = prefix(operation, component);
    return prefix.isEmpty() ? "" : prefix + "_";
  }

  @Nullable
  private SearchComponent componentForIndexName(
      @Nonnull OperationFingerprint operation, @Nonnull String indexOrBase) {
    if (IndexConvention.matchesSemanticEntityIndexFamily(indexOrBase)) {
      return SearchComponent.SEMANTIC;
    }
    if (IndexConvention.matchesV3EntityIndexFamily(indexOrBase)) {
      return SearchComponent.SEARCH_V3;
    }
    if (IndexConvention.matchesV2EntityIndexFamily(indexOrBase)) {
      return SearchComponent.SEARCH_V2;
    }
    if (IndexConvention.matchesTimeseriesAspectIndexFamily(indexOrBase)) {
      return SearchComponent.TIMESERIES;
    }
    String lower = indexOrBase.toLowerCase(Locale.ROOT);
    if (lower.equals(DATAHUB_USAGE_EVENT_INDEX)
        || lower.equals(
            getIndexNameForComponent(
                operation, DATAHUB_USAGE_EVENT_INDEX, SearchComponent.USAGE))) {
      return SearchComponent.USAGE;
    }
    if (lower.equals(GRAPH_SERVICE_INDEX) || lower.endsWith("_" + GRAPH_SERVICE_INDEX)) {
      return SearchComponent.GRAPH;
    }
    if (lower.equals(SYSTEM_METADATA_SERVICE_INDEX)
        || lower.endsWith("_" + SYSTEM_METADATA_SERVICE_INDEX)) {
      return SearchComponent.SYSTEM_METADATA;
    }
    return null;
  }

  private List<String> buildEntityIndicesPatterns(@Nonnull OperationFingerprint operation) {
    List<String> patterns = new ArrayList<>();

    if (isV2Enabled(entityIndexConfiguration)) {
      patterns.add(buildV2Pattern(operation));
    }

    if (isV3Enabled(entityIndexConfiguration)) {
      patterns.add(buildV3Pattern(operation));
    }

    return patterns;
  }

  private String buildV2Pattern(@Nonnull OperationFingerprint operation) {
    return (prefixToken(operation, SearchComponent.SEARCH_V2)
        + "*"
        + ENTITY_INDEX_SUFFIX
        + "_"
        + ENTITY_INDEX_VERSION);
  }

  private String buildV3Pattern(@Nonnull OperationFingerprint operation) {
    return (prefixToken(operation, SearchComponent.SEARCH_V3)
        + "*"
        + ENTITY_INDEX_SUFFIX
        + "_"
        + ENTITY_INDEX_VERSION_V3);
  }

  @Nonnull
  @Override
  public String getIdHashAlgo() {
    return indexConventionConfig.getHashIdAlgo();
  }

  private Optional<String> extractIndexBase(
      @Nonnull OperationFingerprint operation,
      String indexName,
      String indexSuffix,
      @Nullable SearchComponent component) {
    String prefixString = prefixToken(operation, component);
    if (!indexName.startsWith(prefixString)) {
      return Optional.empty();
    }
    int prefixIndex = prefixString.length();
    int suffixIndex = indexName.indexOf(indexSuffix);
    if (prefixIndex < suffixIndex) {
      return Optional.of(indexName.substring(prefixIndex, suffixIndex));
    }
    return Optional.empty();
  }

  private Optional<String> extractEntityName(
      @Nonnull OperationFingerprint operation, String indexName) {
    return extractIndexBase(
        operation,
        indexName,
        ENTITY_INDEX_SUFFIX + "_" + ENTITY_INDEX_VERSION,
        SearchComponent.SEARCH_V2);
  }

  private Optional<String> extractEntityNameSemantic(
      @Nonnull OperationFingerprint operation, String semanticIndexName) {
    return extractIndexBase(
        operation,
        semanticIndexName,
        ENTITY_INDEX_SUFFIX + "_" + ENTITY_INDEX_VERSION + "_" + SEMANTIC_INDEX_SUFFIX,
        SearchComponent.SEMANTIC);
  }

  @Override
  public Optional<String> getPrefix(@Nonnull OperationFingerprint operation) {
    String prefix = prefix(operation);
    return prefix.isEmpty() ? Optional.empty() : Optional.of(prefix);
  }

  @Override
  public Optional<String> getPrefix(
      @Nonnull OperationFingerprint operation, @Nonnull SearchComponent component) {
    String prefix = prefix(operation, component);
    return prefix.isEmpty() ? Optional.empty() : Optional.of(prefix);
  }

  @Nonnull
  @Override
  public String getIndexName(
      @Nonnull OperationFingerprint operation, Class<? extends RecordTemplate> documentClass) {
    return this.getIndexName(operation, documentClass.getSimpleName());
  }

  @Nonnull
  @Override
  public String getIndexName(@Nonnull OperationFingerprint operation, EntitySpec entitySpec) {
    return getEntityIndexName(operation, entitySpec.getName());
  }

  @Nonnull
  @Override
  public String getIndexName(@Nonnull OperationFingerprint operation, String baseIndexName) {
    return getIndexNameForComponent(
        operation, baseIndexName, componentForIndexName(operation, baseIndexName));
  }

  @Nonnull
  @Override
  public String getIndexName(
      @Nonnull OperationFingerprint operation,
      @Nonnull SearchComponent component,
      @Nonnull String baseIndexName) {
    return getIndexNameForComponent(operation, baseIndexName, component);
  }

  @Nonnull
  private String getIndexNameForComponent(
      @Nonnull OperationFingerprint operation,
      @Nonnull String baseIndexName,
      @Nullable SearchComponent component) {
    // Resolve the prefix ONCE, then derive both the cache key and the cached value from the same
    // token — otherwise a pluggable resolver observing a routing refresh between the two calls
    // could
    // cache a value under a mismatched key and target the wrong index until LRU eviction.
    final String prefixToken = prefixToken(operation, component);
    final String key = prefixToken + baseIndexName;
    // Lock-free hit fast-path: hits never evict.
    final String cached = indexNameMapping.get(key);
    if (cached != null) {
      return cached;
    }
    final String value = key.toLowerCase(Locale.ROOT);
    // Bounded to cap growth (prefixes × base names) on this singleton; clear only on a miss at the
    // cap. A miss just recomputes the cheap name, so an occasional flush is cheap.
    if (indexNameMapping.size() >= INDEX_NAME_CACHE_MAX_SIZE) {
      indexNameMapping.clear();
    }
    indexNameMapping.putIfAbsent(key, value);
    return value;
  }

  @Nonnull
  @Override
  public String getEntityIndexName(@Nonnull OperationFingerprint operation, String entityName) {
    return this.getIndexName(
        operation, entityName + ENTITY_INDEX_SUFFIX + "_" + ENTITY_INDEX_VERSION);
  }

  @Nonnull
  @Override
  public String getEntityIndexNameSemantic(
      @Nonnull OperationFingerprint operation, String entityName) {
    return this.getIndexName(
        operation,
        entityName
            + ENTITY_INDEX_SUFFIX
            + "_"
            + ENTITY_INDEX_VERSION
            + "_"
            + SEMANTIC_INDEX_SUFFIX);
  }

  @Nonnull
  @Override
  public String getEntityIndexNameV3(@Nonnull OperationFingerprint operation, String searchGroup) {
    return this.getIndexName(
        operation, searchGroup + ENTITY_INDEX_SUFFIX + "_" + ENTITY_INDEX_VERSION_V3);
  }

  @Nonnull
  @Override
  public String getTimeseriesAspectIndexName(
      @Nonnull OperationFingerprint operation, String entityName, String aspectName) {
    return this.getIndexName(operation, SearchComponent.TIMESERIES, entityName + "_" + aspectName)
        + TIMESERIES_INDEX_MARKER;
  }

  @Nonnull
  @Override
  public List<String> getAllEntityIndicesPatterns(@Nonnull OperationFingerprint operation) {
    return buildEntityIndicesPatterns(operation);
  }

  @Nonnull
  @Override
  public List<String> getV3EntityIndexPatterns(@Nonnull OperationFingerprint operation) {
    return List.of(buildV3Pattern(operation));
  }

  @Nonnull
  @Override
  public String getAllTimeseriesAspectIndicesPattern(@Nonnull OperationFingerprint operation) {
    return prefixToken(operation, SearchComponent.TIMESERIES) + "*" + TIMESERIES_INDEX_MARKER;
  }

  @Nonnull
  @Override
  public String getAllSemanticEntityIndicesPattern(@Nonnull OperationFingerprint operation) {
    return prefixToken(operation, SearchComponent.SEMANTIC) + "*index_v2_semantic";
  }

  @Nonnull
  @Override
  public List<String> getEntityIndicesCleanupPatterns(
      @Nonnull OperationFingerprint operation,
      @Nonnull EntityIndexConfiguration entityIndexConfiguration) {
    List<String> patterns = new ArrayList<>();

    // Add V2 pattern if V2 is enabled and cleanup is enabled
    if (isV2Enabled(entityIndexConfiguration) && isV2CleanupEnabled(entityIndexConfiguration)) {
      patterns.add(buildV2Pattern(operation));
    }

    // Add V3 pattern if V3 is enabled and cleanup is enabled
    if (isV3Enabled(entityIndexConfiguration) && isV3CleanupEnabled(entityIndexConfiguration)) {
      patterns.add(buildV3Pattern(operation));
    }

    return patterns;
  }

  @Override
  public Optional<String> getEntityName(@Nonnull OperationFingerprint operation, String indexName) {
    return extractEntityName(operation, indexName);
  }

  @Override
  public Optional<String> getEntityNameSemantic(
      @Nonnull OperationFingerprint operation, String semanticIndexName) {
    return extractEntityNameSemantic(operation, semanticIndexName);
  }

  @Override
  public Optional<Pair<String, String>> getEntityAndAspectName(
      @Nonnull OperationFingerprint operation, String timeseriesAspectIndexName) {
    Optional<String> entityAndAspect =
        extractIndexBase(
            operation,
            timeseriesAspectIndexName,
            TIMESERIES_INDEX_MARKER,
            SearchComponent.TIMESERIES);
    if (entityAndAspect.isPresent()) {
      String[] entityAndAspectTokens = entityAndAspect.get().split("_");
      if (entityAndAspectTokens.length == 2) {
        return Optional.of(Pair.of(entityAndAspectTokens[0], entityAndAspectTokens[1]));
      }
    }
    return Optional.empty();
  }

  @Nonnull
  @Override
  public String getEntityDocumentId(Urn entityUrn) {
    final String unencodedId;
    if (indexConventionConfig.schemaFieldDocIdHashEnabled
        && SCHEMA_FIELD_ENTITY_NAME.equals(entityUrn.getEntityType())) {
      unencodedId = SchemaFieldUtils.generateDocumentId(entityUrn);
    } else {
      unencodedId = entityUrn.toString();
    }

    return URLEncoder.encode(unencodedId, StandardCharsets.UTF_8);
  }

  /** Checks if V2 entity index is enabled based on configuration */
  private boolean isV2Enabled(@Nonnull EntityIndexConfiguration entityIndexConfiguration) {
    return (entityIndexConfiguration.getV2() != null
        && entityIndexConfiguration.getV2().isEnabled());
  }

  /** Checks if V3 entity index is enabled based on configuration */
  private boolean isV3Enabled(@Nonnull EntityIndexConfiguration entityIndexConfiguration) {
    return (entityIndexConfiguration.getV3() != null
        && entityIndexConfiguration.getV3().isEnabled());
  }

  /** Checks if V2 cleanup is enabled based on configuration */
  private boolean isV2CleanupEnabled(@Nonnull EntityIndexConfiguration entityIndexConfiguration) {
    return (entityIndexConfiguration.getV2() != null
        && entityIndexConfiguration.getV2().isCleanup());
  }

  /** Checks if V3 cleanup is enabled based on configuration */
  private boolean isV3CleanupEnabled(@Nonnull EntityIndexConfiguration entityIndexConfiguration) {
    return (entityIndexConfiguration.getV3() != null
        && entityIndexConfiguration.getV3().isCleanup());
  }

  /**
   * Helper method to check if an index name matches the entity index pattern with a given suffix.
   *
   * @param operation operation whose prefix the index name is expected to carry
   * @param indexName the index name to check
   * @param suffix the expected suffix (e.g., "index_v2", "index_v3", "index_v2_semantic")
   * @return true if the index name matches the pattern
   */
  private boolean isEntityIndexWithSuffix(
      @Nonnull OperationFingerprint operation, @Nonnull String indexName, String suffix) {
    if (!indexName.endsWith(suffix)) {
      return false;
    }
    // Check that there's at least one character before the suffix
    int suffixStart = indexName.length() - suffix.length();
    if (suffixStart <= 0) {
      return false;
    }

    // If we have a prefix configured for this operation, check that the index name starts with it
    SearchComponent component = componentForIndexName(operation, indexName);
    String prefix = prefix(operation, component);
    if (!prefix.isEmpty()) {
      return indexName.startsWith(prefix + "_");
    }

    return true;
  }

  @Override
  public boolean isV2EntityIndex(
      @Nonnull OperationFingerprint operation, @Nonnull String indexName) {
    // Pattern: [prefix]_[entityName]index_v2
    return isEntityIndexWithSuffix(
        operation, indexName, ENTITY_INDEX_SUFFIX + "_" + ENTITY_INDEX_VERSION);
  }

  @Override
  public boolean isV3EntityIndex(
      @Nonnull OperationFingerprint operation, @Nonnull String indexName) {
    // Pattern: [prefix]_[entityName]index_v3
    return isEntityIndexWithSuffix(
        operation, indexName, ENTITY_INDEX_SUFFIX + "_" + ENTITY_INDEX_VERSION_V3);
  }

  @Override
  public boolean isSemanticEntityIndex(
      @Nonnull OperationFingerprint operation, @Nonnull String indexName) {
    // Pattern: [prefix]_[entityName]index_v2_semantic
    return isEntityIndexWithSuffix(
        operation,
        indexName,
        ENTITY_INDEX_SUFFIX + "_" + ENTITY_INDEX_VERSION + "_" + SEMANTIC_INDEX_SUFFIX);
  }

  /** Since this is used outside of Spring */
  @Value
  @Builder
  public static class IndexConventionConfig {

    @Builder.Default String hashIdAlgo = "MD5";

    @Builder.Default boolean schemaFieldDocIdHashEnabled = false;
  }
}
