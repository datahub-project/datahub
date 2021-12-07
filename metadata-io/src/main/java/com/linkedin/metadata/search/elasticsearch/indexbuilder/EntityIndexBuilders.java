package com.linkedin.metadata.search.elasticsearch.indexbuilder;

import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import java.io.IOException;
import lombok.RequiredArgsConstructor;


@RequiredArgsConstructor
public class EntityIndexBuilders {
  private final ESIndexBuilder indexBuilder;
  private final EntityRegistry entityRegistry;
  private final IndexConvention indexConvention;
  private final SettingsBuilder settingsBuilder;

  public void buildAll() {
    for (EntitySpec entitySpec : entityRegistry.getEntitySpecs().values()) {
      try {
        new EntityIndexBuilder(indexBuilder, entitySpec, settingsBuilder,
            indexConvention.getIndexName(entitySpec)).buildIndex();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }
}
