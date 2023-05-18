package com.linkedin.metadata.utils.elasticsearch;

import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.models.EntitySpec;
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
  // Map from Index name -> Entity name (inverse of above)
  private final Map<String, String> entityNameMapping = new ConcurrentHashMap<>();
  private final Optional<String> _prefix;
  private final String _getAllEntityIndicesPattern;
  private final String _getAllTimeseriesIndicesPattern;

  private final static String ENTITY_INDEX_VERSION = "v2";
  private final static String ENTITY_INDEX_SUFFIX = "index";
  private final static String TIMESERIES_INDEX_VERSION = "v1";
  private final static String TIMESERIES_ENTITY_INDEX_SUFFIX = "aspect";

  public IndexConventionImpl(@Nullable String prefix) {
    _prefix = StringUtils.isEmpty(prefix) ? Optional.empty() : Optional.of(prefix);
    _getAllEntityIndicesPattern =
        _prefix.map(p -> p + "_").orElse("") + "*" + ENTITY_INDEX_SUFFIX + "_" + ENTITY_INDEX_VERSION;
    _getAllTimeseriesIndicesPattern =
        _prefix.map(p -> p + "_").orElse("") + "*" + TIMESERIES_ENTITY_INDEX_SUFFIX + "_" + TIMESERIES_INDEX_VERSION;
  }

  private String createIndexName(String baseName) {
    return (_prefix.map(prefix -> prefix + "_").orElse("") + baseName).toLowerCase();
  }

  private String extractEntityName(String indexName) {
    // TODO(indy): Test this
    String indexSuffix = ENTITY_INDEX_SUFFIX + "_" + ENTITY_INDEX_VERSION;
    return indexName.substring(
        _prefix.map(prefix -> prefix + "_").orElse("").length(),
        indexName.indexOf(indexSuffix));
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
    String indexName = this.getIndexName(entityName + ENTITY_INDEX_SUFFIX + "_" + ENTITY_INDEX_VERSION);
    entityNameMapping.putIfAbsent(entityName, indexName);
    return indexName;
  }

  @Nonnull
  @Override
  public String getTimeseriesAspectIndexName(String entityName, String aspectName) {
    return this.getIndexName(entityName + "_" + aspectName) + TIMESERIES_ENTITY_INDEX_SUFFIX + "_"
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

  @Nullable
  @Override
  public String getEntityName(String indexName) {
    return extractEntityName(indexName);
  }
}
