package com.linkedin.metadata.timeseries.elastic.indexbuilder;

import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ESIndexBuilder;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import java.io.IOException;
import java.util.Collections;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;


@Slf4j
@RequiredArgsConstructor
public class TimeseriesAspectIndexBuilders {
  private final ESIndexBuilder _indexBuilder;
  private final EntityRegistry _entityRegistry;
  private final IndexConvention _indexConvention;

  public void buildAll() {
    for (EntitySpec entitySpec : _entityRegistry.getEntitySpecs().values()) {
      for (AspectSpec aspectSpec : entitySpec.getAspectSpecs()) {
        if (aspectSpec.isTimeseries()) {
          try {
            _indexBuilder.buildIndex(
                _indexConvention.getTimeseriesAspectIndexName(entitySpec.getName(), aspectSpec.getName()),
                MappingsBuilder.getMappings(aspectSpec), Collections.emptyMap());
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
