package com.linkedin.metadata.search.elasticsearch.indexbuilder;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.shared.ElasticSearchIndexed;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.structured.StructuredPropertyDefinition;
import com.linkedin.util.Pair;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@RequiredArgsConstructor
@Slf4j
public class EntityIndexBuilders implements ElasticSearchIndexed {
  private final ESIndexBuilder indexBuilder;
  private final EntityRegistry entityRegistry;
  @Getter private final IndexConvention indexConvention;
  private final SettingsBuilder settingsBuilder;

  public ESIndexBuilder getIndexBuilder() {
    return indexBuilder;
  }

  @Override
  public void reindexAll(Collection<Pair<Urn, StructuredPropertyDefinition>> properties) {
    for (ReindexConfig config : buildReindexConfigs(properties)) {
      try {
        indexBuilder.buildIndex(config);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public List<ReindexConfig> buildReindexConfigs(
      Collection<Pair<Urn, StructuredPropertyDefinition>> properties) {
    Map<String, Object> settings = settingsBuilder.getSettings();

    return entityRegistry.getEntitySpecs().values().stream()
        .map(
            entitySpec -> {
              try {
                Map<String, Object> mappings =
                    MappingsBuilder.getMappings(entityRegistry, entitySpec, properties);
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
      Urn urn, StructuredPropertyDefinition property) {
    Map<String, Object> settings = settingsBuilder.getSettings();

    return entityRegistry.getEntitySpecs().values().stream()
        .map(
            entitySpec -> {
              try {
                Map<String, Object> mappings =
                    MappingsBuilder.getMappings(
                        entityRegistry, entitySpec, List.of(Pair.of(urn, property)));
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
