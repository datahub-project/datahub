package com.linkedin.metadata.search.elasticsearch.indexbuilder;

import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.shared.ElasticSearchIndexed;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.structured.StructuredPropertyDefinition;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@RequiredArgsConstructor
@Slf4j
public class EntityIndexBuilders implements ElasticSearchIndexed {
  private final ESIndexBuilder indexBuilder;
  private final EntityRegistry entityRegistry;
  private final IndexConvention indexConvention;
  private final SettingsBuilder settingsBuilder;

  public ESIndexBuilder getIndexBuilder() {
    return indexBuilder;
  }

  @Override
  public void reindexAll() {
    for (ReindexConfig config : buildReindexConfigs()) {
      try {
        indexBuilder.buildIndex(config);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public List<ReindexConfig> buildReindexConfigs() {
    Map<String, Object> settings = settingsBuilder.getSettings();
    MappingsBuilder.setEntityRegistry(entityRegistry);
    return entityRegistry.getEntitySpecs().values().stream()
        .map(
            entitySpec -> {
              try {
                Map<String, Object> mappings = MappingsBuilder.getMappings(entitySpec);
                return indexBuilder.buildReindexState(
                    indexConvention.getIndexName(entitySpec), mappings, settings, true);
              } catch (IOException e) {
                throw new RuntimeException(e);
              }
            })
        .collect(Collectors.toList());
  }

  @Override
  public List<ReindexConfig> buildReindexConfigsWithAllStructProps(
      Collection<StructuredPropertyDefinition> properties) {
    Map<String, Object> settings = settingsBuilder.getSettings();
    MappingsBuilder.setEntityRegistry(entityRegistry);
    return entityRegistry.getEntitySpecs().values().stream()
        .map(
            entitySpec -> {
              try {
                Map<String, Object> mappings = MappingsBuilder.getMappings(entitySpec, properties);
                return indexBuilder.buildReindexState(
                    indexConvention.getIndexName(entitySpec), mappings, settings);
              } catch (IOException e) {
                throw new RuntimeException(e);
              }
            })
        .collect(Collectors.toList());
  }

  /**
   * Given a structured property generate all entity index configurations impacted by it, preserving
   * existing properties
   *
   * @param property the new property
   * @return index configurations impacted by the new property
   */
  public List<ReindexConfig> buildReindexConfigsWithNewStructProp(
      StructuredPropertyDefinition property) {
    Map<String, Object> settings = settingsBuilder.getSettings();
    MappingsBuilder.setEntityRegistry(entityRegistry);
    return entityRegistry.getEntitySpecs().values().stream()
        .map(
            entitySpec -> {
              try {
                Map<String, Object> mappings =
                    MappingsBuilder.getMappings(entitySpec, List.of(property));
                return indexBuilder.buildReindexState(
                    indexConvention.getIndexName(entitySpec), mappings, settings, true);
              } catch (IOException e) {
                throw new RuntimeException(e);
              }
            })
        .filter(Objects::nonNull)
        .filter(ReindexConfig::hasNewStructuredProperty)
        .collect(Collectors.toList());
  }
}
