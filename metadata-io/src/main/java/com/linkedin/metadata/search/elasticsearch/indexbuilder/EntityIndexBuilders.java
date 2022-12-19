package com.linkedin.metadata.search.elasticsearch.indexbuilder;

import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import java.io.IOException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;


@Slf4j
@RequiredArgsConstructor
public class EntityIndexBuilders {
  private final ESIndexBuilder indexBuilder;
  private final EntityRegistry entityRegistry;
  private final IndexConvention indexConvention;
  private final SettingsBuilder settingsBuilder;
  private boolean enabled;

  public void buildAll() {
    for (EntitySpec entitySpec : entityRegistry.getEntitySpecs().values()) {
      try {
        new EntityIndexBuilder(indexBuilder, entitySpec, settingsBuilder,
                indexConvention.getIndexName(entitySpec), enabled).buildIndex();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }
}
