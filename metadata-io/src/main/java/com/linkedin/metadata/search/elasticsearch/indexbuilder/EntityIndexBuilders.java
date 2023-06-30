package com.linkedin.metadata.search.elasticsearch.indexbuilder;

import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.shared.ElasticSearchIndexed;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import java.io.IOException;
import java.util.List;
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

  @Override
  public void reindexAll() {
      for (ReindexConfig config : getReindexConfigs()) {
          try {
              indexBuilder.buildIndex(config);
          } catch (IOException e) {
              throw new RuntimeException(e);
          }
      }
  }

  @Override
  public List<ReindexConfig> getReindexConfigs() {
    return entityRegistry.getEntitySpecs().values().stream().flatMap(entitySpec -> {
                      try {
                        return new EntityIndexBuilder(indexBuilder, entitySpec, settingsBuilder, indexConvention.getIndexName(entitySpec))
                                .getReindexConfigs().stream();
                      } catch (IOException e) {
                        throw new RuntimeException(e);
                      }
                    }
            ).collect(Collectors.toList());
  }
}
