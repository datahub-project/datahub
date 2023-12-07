package com.linkedin.metadata.utils.elasticsearch;

import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.util.Pair;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;

// Default implementation of search index naming convention
public class IndexConventionImpl implements IndexConvention {
  // Map from Entity name -> Index name
  private final Map<String, String> indexNameMapping = new ConcurrentHashMap<>();
  private final Optional<String> _prefix;
  private final String _getAllEntityIndicesPattern;
  private final String _getAllTimeseriesIndicesPattern;

  private static final String ENTITY_INDEX_VERSION = "v2";
  private static final String ENTITY_INDEX_SUFFIX = "index";
  private static final String TIMESERIES_INDEX_VERSION = "v1";
  private static final String TIMESERIES_ENTITY_INDEX_SUFFIX = "aspect";

  public IndexConventionImpl(@Nullable String prefix) {
    _prefix = StringUtils.isEmpty(prefix) ? Optional.empty() : Optional.of(prefix);
    _getAllEntityIndicesPattern =
        _prefix.map(p -> p + "_").orElse("")
            + "*"
            + ENTITY_INDEX_SUFFIX
            + "_"
            + ENTITY_INDEX_VERSION;
    _getAllTimeseriesIndicesPattern =
        _prefix.map(p -> p + "_").orElse("")
            + "*"
            + TIMESERIES_ENTITY_INDEX_SUFFIX
            + "_"
            + TIMESERIES_INDEX_VERSION;
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
  public String getTimeseriesAspectIndexName(String entityName, String aspectName) {
    return this.getIndexName(entityName + "_" + aspectName)
        + TIMESERIES_ENTITY_INDEX_SUFFIX
        + "_"
        + TIMESERIES_INDEX_VERSION;
  }

  @Nonnull
  @Override
  public String getAllEntityIndicesPattern() {
    return _getAllEntityIndicesPattern;
  }

  @Nonnull
  @Override
  public String getAllTimeseriesAspectIndicesPattern() {
    return _getAllTimeseriesIndicesPattern;
  }

  @Override
  public Optional<String> getEntityName(String indexName) {
    return extractEntityName(indexName);
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
}
