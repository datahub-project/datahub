package com.linkedin.gms.factory.entity.update.indices;

import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.gms.factory.search.EntityIndexBuildersFactory;
import com.linkedin.metadata.client.EntityClientAspectRetriever;
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

  @Value("${entityClient.impl:java}")
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
            searchDocumentTransformer,
            entityIndexBuilders);

    if ("restli".equals(entityClientImpl)) {
      /*
       When restli mode the EntityService is not available. Wire in an AspectRetriever here instead
       based on the entity client
      */
      SystemEntityClient systemEntityClient = context.getBean(SystemEntityClient.class);
      updateIndicesService.initializeAspectRetriever(
          EntityClientAspectRetriever.builder()
              .entityRegistry(entityRegistry)
              .entityClient(systemEntityClient)
              .build());
    }

    return updateIndicesService;
  }
}
