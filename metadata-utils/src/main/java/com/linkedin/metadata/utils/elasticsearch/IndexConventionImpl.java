package com.linkedin.metadata.utils.elasticsearch;

import static com.linkedin.metadata.Constants.SCHEMA_FIELD_ENTITY_NAME;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.config.search.EntityIndexConfiguration;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.utils.SchemaFieldUtils;
import com.linkedin.util.Pair;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
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

  public static IndexConvention noPrefix(
      @Nonnull String idHashAlgo, @Nonnull EntityIndexConfiguration entityIndexConfiguration) {
    return new IndexConventionImpl(
        IndexConventionConfig.builder().hashIdAlgo(idHashAlgo).build(), entityIndexConfiguration);
  }

  // Map from Entity name -> Index name
  private final Map<String, String> indexNameMapping = new ConcurrentHashMap<>();
  private final Optional<String> _prefix;
  private final List<String> _getAllEntityIndicesPatterns;
  private final String _getAllTimeseriesIndicesPattern;
  private final String _v3EntityIndexPatterns;

  @Getter private final IndexConventionConfig indexConventionConfig;

  private final EntityIndexConfiguration entityIndexConfiguration;

  private static final String ENTITY_INDEX_VERSION = "v2";
  private static final String ENTITY_INDEX_VERSION_V3 = "v3";
  private static final String ENTITY_INDEX_SUFFIX = "index";
  private static final String SEMANTIC_INDEX_SUFFIX = "semantic";
  private static final String TIMESERIES_INDEX_VERSION = "v1";
  private static final String TIMESERIES_ENTITY_INDEX_SUFFIX = "aspect";

  public IndexConventionImpl(
      IndexConventionConfig indexConventionConfig,
      @Nonnull EntityIndexConfiguration entityIndexConfiguration) {
    this.indexConventionConfig = indexConventionConfig;
    this.entityIndexConfiguration = entityIndexConfiguration;
    _prefix =
        StringUtils.isEmpty(indexConventionConfig.getPrefix())
            ? Optional.empty()
            : Optional.of(indexConventionConfig.getPrefix());
    // Build patterns based on configuration
    _getAllEntityIndicesPatterns = buildEntityIndicesPatterns();
    _getAllTimeseriesIndicesPattern =
        _prefix.map(p -> p + "_").orElse("")
            + "*"
            + TIMESERIES_ENTITY_INDEX_SUFFIX
            + "_"
            + TIMESERIES_INDEX_VERSION;
    _v3EntityIndexPatterns = buildV3Pattern();
  }

  private List<String> buildEntityIndicesPatterns() {
    List<String> patterns = new ArrayList<>();

    if (isV2Enabled(entityIndexConfiguration)) {
      patterns.add(buildV2Pattern());
    }

    if (isV3Enabled(entityIndexConfiguration)) {
      patterns.add(buildV3Pattern());
    }

    return patterns;
  }

  private String buildV2Pattern() {
    return (_prefix.map(p -> p + "_").orElse("")
        + "*"
        + ENTITY_INDEX_SUFFIX
        + "_"
        + ENTITY_INDEX_VERSION);
  }

  private String buildV3Pattern() {
    return (_prefix.map(p -> p + "_").orElse("")
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

  private String createIndexName(String baseName) {
    return (_prefix.map(prefix -> prefix + "_").orElse("") + baseName).toLowerCase();
  }

  private Optional<String> extractIndexBase(String indexName, String indexSuffix) {
    String prefixString = _prefix.map(prefix -> prefix + "_").orElse("");
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

  private Optional<String> extractEntityName(String indexName) {
    return extractIndexBase(indexName, ENTITY_INDEX_SUFFIX + "_" + ENTITY_INDEX_VERSION);
  }

  private Optional<String> extractEntityNameSemantic(String semanticIndexName) {
    return extractIndexBase(
        semanticIndexName,
        ENTITY_INDEX_SUFFIX + "_" + ENTITY_INDEX_VERSION + "_" + SEMANTIC_INDEX_SUFFIX);
  }

  @Override
  public Optional<String> getPrefix() {
    return _prefix;
  }

  @Nonnull
  @Override
  public String getIndexName(Class<? extends RecordTemplate> documentClass) {
    return this.getIndexName(documentClass.getSimpleName());
  }

  @Nonnull
  @Override
  public String getIndexName(EntitySpec entitySpec) {
    return getEntityIndexName(entitySpec.getName());
  }

  @Nonnull
  @Override
  public String getIndexName(String baseIndexName) {
    return indexNameMapping.computeIfAbsent(baseIndexName, this::createIndexName);
  }

  @Nonnull
  @Override
  public String getEntityIndexName(String entityName) {
    return this.getIndexName(entityName + ENTITY_INDEX_SUFFIX + "_" + ENTITY_INDEX_VERSION);
  }

  @Nonnull
  @Override
  public String getEntityIndexNameSemantic(String entityName) {
    return this.getIndexName(
        entityName
            + ENTITY_INDEX_SUFFIX
            + "_"
            + ENTITY_INDEX_VERSION
            + "_"
            + SEMANTIC_INDEX_SUFFIX);
  }

  @Nonnull
  @Override
  public String getEntityIndexNameV3(String searchGroup) {
    return this.getIndexName(searchGroup + ENTITY_INDEX_SUFFIX + "_" + ENTITY_INDEX_VERSION_V3);
  }

  @Nonnull
  @Override
  public String getTimeseriesAspectIndexName(String entityName, String aspectName) {
    return (this.getIndexName(entityName + "_" + aspectName)
        + TIMESERIES_ENTITY_INDEX_SUFFIX
        + "_"
        + TIMESERIES_INDEX_VERSION);
  }

  @Nonnull
  @Override
  public List<String> getAllEntityIndicesPatterns() {
    return _getAllEntityIndicesPatterns;
  }

  @Nonnull
  @Override
  public List<String> getV3EntityIndexPatterns() {
    return List.of(_v3EntityIndexPatterns);
  }

  @Nonnull
  @Override
  public String getAllTimeseriesAspectIndicesPattern() {
    return _getAllTimeseriesIndicesPattern;
  }

  @Nonnull
  @Override
  public List<String> getEntityIndicesCleanupPatterns(
      @Nonnull EntityIndexConfiguration entityIndexConfiguration) {
    List<String> patterns = new ArrayList<>();

    // Add V2 pattern if V2 is enabled and cleanup is enabled
    if (isV2Enabled(entityIndexConfiguration) && isV2CleanupEnabled(entityIndexConfiguration)) {
      patterns.add(buildV2Pattern());
    }

    // Add V3 pattern if V3 is enabled and cleanup is enabled
    if (isV3Enabled(entityIndexConfiguration) && isV3CleanupEnabled(entityIndexConfiguration)) {
      patterns.add(buildV3Pattern());
    }

    return patterns;
  }

  @Override
  public Optional<String> getEntityName(String indexName) {
    return extractEntityName(indexName);
  }

  @Override
  public Optional<String> getEntityNameSemantic(String semanticIndexName) {
    return extractEntityNameSemantic(semanticIndexName);
  }

  @Override
  public Optional<Pair<String, String>> getEntityAndAspectName(String timeseriesAspectIndexName) {
    Optional<String> entityAndAspect =
        extractIndexBase(
            timeseriesAspectIndexName,
            TIMESERIES_ENTITY_INDEX_SUFFIX + "_" + TIMESERIES_INDEX_VERSION);
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
   * @param indexName the index name to check
   * @param suffix the expected suffix (e.g., "index_v2", "index_v3", "index_v2_semantic")
   * @return true if the index name matches the pattern
   */
  private boolean isEntityIndexWithSuffix(@Nonnull String indexName, String suffix) {
    if (!indexName.endsWith(suffix)) {
      return false;
    }
    // Check that there's at least one character before the suffix
    int suffixStart = indexName.length() - suffix.length();
    if (suffixStart <= 0) {
      return false;
    }

    // If we have a prefix configured, check that the index name starts with it
    if (_prefix.isPresent()) {
      String expectedPrefix = _prefix.get() + "_";
      return indexName.startsWith(expectedPrefix);
    }

    return true;
  }

  @Override
  public boolean isV2EntityIndex(@Nonnull String indexName) {
    // Pattern: [prefix]_[entityName]index_v2
    return isEntityIndexWithSuffix(indexName, ENTITY_INDEX_SUFFIX + "_" + ENTITY_INDEX_VERSION);
  }

  @Override
  public boolean isV3EntityIndex(@Nonnull String indexName) {
    // Pattern: [prefix]_[entityName]index_v3
    return isEntityIndexWithSuffix(indexName, ENTITY_INDEX_SUFFIX + "_" + ENTITY_INDEX_VERSION_V3);
  }

  @Override
  public boolean isSemanticEntityIndex(@Nonnull String indexName) {
    // Pattern: [prefix]_[entityName]index_v2_semantic
    return isEntityIndexWithSuffix(
        indexName, ENTITY_INDEX_SUFFIX + "_" + ENTITY_INDEX_VERSION + "_" + SEMANTIC_INDEX_SUFFIX);
  }

  /** Since this is used outside of Spring */
  @Value
  @Builder
  public static class IndexConventionConfig {

    @Builder.Default String hashIdAlgo = "MD5";

    @Builder.Default @Nullable String prefix = null;

    @Builder.Default boolean schemaFieldDocIdHashEnabled = false;
  }
}
