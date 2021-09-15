package com.linkedin.metadata.timeseries.elastic.indexbuilder;

import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.IndexBuilder;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import java.io.IOException;
import java.util.Collections;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.client.RestHighLevelClient;


@Slf4j
@RequiredArgsConstructor
public class TimeseriesAspectIndexBuilders {
  private final EntityRegistry _entityRegistry;
  private final RestHighLevelClient _searchClient;
  private final IndexConvention _indexConvention;

  public void buildAll() {
    for (EntitySpec entitySpec : _entityRegistry.getEntitySpecs().values()) {
      for (AspectSpec aspectSpec : entitySpec.getAspectSpecs()) {
        if (aspectSpec.isTimeseries()) {
          try {
            new IndexBuilder(_searchClient,
                _indexConvention.getTimeseriesAspectIndexName(entitySpec.getName(), aspectSpec.getName()),
                MappingsBuilder.getMappings(aspectSpec), Collections.emptyMap()).buildIndex();
          } catch (IOException e) {
            log.error("Issue while building timeseries field index for entity {} aspect {}", entitySpec.getName(),
                aspectSpec.getName());
            log.error("Exception: ", e);
          }
        }
      }
    }
  }
}
