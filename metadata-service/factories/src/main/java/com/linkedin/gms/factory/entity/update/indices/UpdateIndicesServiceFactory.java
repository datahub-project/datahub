package com.linkedin.gms.factory.entity.update.indices;

import com.linkedin.entity.client.SystemRestliEntityClient;
import com.linkedin.gms.factory.search.EntityIndexBuildersFactory;
import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.EntityIndexBuilders;
import com.linkedin.metadata.search.transformer.SearchDocumentTransformer;
import com.linkedin.metadata.service.UpdateIndicesService;
import com.linkedin.metadata.systemmetadata.SystemMetadataService;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration
@Import(EntityIndexBuildersFactory.class)
public class UpdateIndicesServiceFactory {
  @Autowired private ApplicationContext context;

  @Value("${entityClient.preferredImpl:java}")
  private String entityClientImpl;

  @Bean
  public UpdateIndicesService updateIndicesService(
      GraphService graphService,
      EntitySearchService entitySearchService,
      TimeseriesAspectService timeseriesAspectService,
      SystemMetadataService systemMetadataService,
      EntityRegistry entityRegistry,
      SearchDocumentTransformer searchDocumentTransformer,
      EntityIndexBuilders entityIndexBuilders) {
    UpdateIndicesService updateIndicesService =
        new UpdateIndicesService(
            graphService,
            entitySearchService,
            timeseriesAspectService,
            systemMetadataService,
            entityRegistry,
            searchDocumentTransformer,
            entityIndexBuilders);

    if ("restli".equals(entityClientImpl)) {
      updateIndicesService.setSystemEntityClient(context.getBean(SystemRestliEntityClient.class));
    }

    return updateIndicesService;
  }
}
