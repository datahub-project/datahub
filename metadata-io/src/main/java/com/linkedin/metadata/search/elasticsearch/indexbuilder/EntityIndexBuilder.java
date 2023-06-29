package com.linkedin.metadata.search.elasticsearch.indexbuilder;

import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.timeseries.BatchWriteOperationsOptions;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import com.linkedin.metadata.shared.ElasticSearchIndexed;
import javax.annotation.Nullable;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.index.query.QueryBuilder;


@Slf4j
@RequiredArgsConstructor
public class EntityIndexBuilder implements ElasticSearchIndexed {
  private final ESIndexBuilder indexBuilder;
  private final EntitySpec entitySpec;
  private final SettingsBuilder settingsBuilder;
  private final String indexName;

  @Override
  public void reindexAll() throws IOException {
    log.info("Setting up index: {}", indexName);
    for (ReindexConfig config : getReindexConfigs()) {
      indexBuilder.buildIndex(config);
    }
  }

  @Override
  public String reindexAsync(String index, @Nullable QueryBuilder filterQuery, BatchWriteOperationsOptions options)
      throws Exception {
    return indexBuilder.reindexInPlaceAsync(index, filterQuery, options);
  }

  @Override
  public List<ReindexConfig> getReindexConfigs() throws IOException {
    Map<String, Object> mappings = MappingsBuilder.getMappings(entitySpec);
    Map<String, Object> settings = settingsBuilder.getSettings();
    return List.of(indexBuilder.buildReindexState(indexName, mappings, settings));
  }
}
