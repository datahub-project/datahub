package com.linkedin.datahub.upgrade.buildindices;

import com.linkedin.gms.factory.search.BaseElasticSearchComponentsFactory;
import com.linkedin.metadata.graph.elastic.ElasticSearchGraphService;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.systemmetadata.ElasticSearchSystemMetadataService;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import java.util.ArrayList;
import java.util.List;


public class IndexUtils {
  private IndexUtils() { }

  private static List<String> _indexNames = new ArrayList<>();

  public static List<String> getAllIndexNames(BaseElasticSearchComponentsFactory.BaseElasticSearchComponents esComponents,
      EntityRegistry entityRegistry) {
    // Avoid locking & reprocessing
    List<String> indexNames = new ArrayList<>(_indexNames);
    if (indexNames.isEmpty()) {
      IndexConvention indexConvention = esComponents.getIndexConvention();
      indexNames.add(indexConvention.getIndexName(ElasticSearchGraphService.INDEX_NAME));
      entityRegistry.getEntitySpecs().values().forEach(entitySpec -> {
        indexNames.add(indexConvention.getEntityIndexName(entitySpec.getName()));
        entitySpec.getAspectSpecs()
            .stream()
            .filter(AspectSpec::isTimeseries)
            .forEach(aspectSpec ->
                indexNames.add(indexConvention.getTimeseriesAspectIndexName(entitySpec.getName(), aspectSpec.getName())));
      });
      indexNames.add(indexConvention.getIndexName(ElasticSearchSystemMetadataService.INDEX_NAME));
      _indexNames = new ArrayList<>(indexNames);
    }

    return indexNames;
  }
}
