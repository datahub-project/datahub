package com.linkedin.gms.factory.entity.update.indices;

import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.search.transformer.SearchDocumentTransformer;
import com.linkedin.metadata.service.UpdateIndicesService;
import com.linkedin.metadata.systemmetadata.SystemMetadataService;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;


@Configuration
public class UpdateIndicesServiceFactory {

  @Bean
  public UpdateIndicesService updateIndicesService(GraphService graphService, EntitySearchService entitySearchService,
      TimeseriesAspectService timeseriesAspectService, SystemMetadataService systemMetadataService,
      EntityRegistry entityRegistry, SearchDocumentTransformer searchDocumentTransformer) {
    return new UpdateIndicesService(graphService, entitySearchService, timeseriesAspectService,
        systemMetadataService, entityRegistry, searchDocumentTransformer);
  }
}
