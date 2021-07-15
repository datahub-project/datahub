package com.linkedin.metadata.search.elasticsearch.indexbuilder;

import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import java.io.IOException;
import lombok.RequiredArgsConstructor;
import org.elasticsearch.client.RestHighLevelClient;

@RequiredArgsConstructor
public class ESIndexBuilders {
  private final EntityRegistry entityRegistry;
  private final RestHighLevelClient searchClient;
  private final IndexConvention indexConvention;

  public void buildAll() {
    for (EntitySpec entitySpec : entityRegistry.getEntitySpecs()) {
      try {
        new EntityIndexBuilder(searchClient, entitySpec, indexConvention.getIndexName(entitySpec)).buildIndex();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }
}
