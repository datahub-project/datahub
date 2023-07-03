package com.linkedin.metadata.search.elasticsearch.indexbuilder;

import com.linkedin.metadata.models.EntitySpec;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import com.linkedin.metadata.shared.ElasticSearchIndexed;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;


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
  public List<ReindexConfig> getReindexConfigs() throws IOException {
    Map<String, Object> mappings = MappingsBuilder.getMappings(entitySpec);
    Map<String, Object> settings = settingsBuilder.getSettings();
    return List.of(indexBuilder.buildReindexState(indexName, mappings, settings));
  }
}
