package com.linkedin.gms.factory.entity.update.indices;

import com.linkedin.gms.factory.search.EntityIndexBuildersFactory;
import com.linkedin.metadata.aspect.CachingAspectRetriever;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.EntityIndexBuilders;
import com.linkedin.metadata.search.transformer.SearchDocumentTransformer;
import com.linkedin.metadata.service.UpdateIndicesService;
import com.linkedin.metadata.systemmetadata.SystemMetadataService;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration
@Import(EntityIndexBuildersFactory.class)
public class UpdateIndicesServiceFactory {

  /*
   When restli mode the EntityService is not available. Wire in an AspectRetriever here instead
   based on the entity client
  */
  @Bean
  @ConditionalOnProperty(name = "entityClient.impl", havingValue = "restli")
  public UpdateIndicesService searchIndicesServiceNonGMS(
      GraphService graphService,
      EntitySearchService entitySearchService,
      TimeseriesAspectService timeseriesAspectService,
      SystemMetadataService systemMetadataService,
      SearchDocumentTransformer searchDocumentTransformer,
      EntityIndexBuilders entityIndexBuilders,
      @Qualifier("cachingAspectRetriever") final CachingAspectRetriever aspectRetriever) {

    UpdateIndicesService updateIndicesService =
        new UpdateIndicesService(
            graphService,
            entitySearchService,
            timeseriesAspectService,
            systemMetadataService,
            searchDocumentTransformer,
            entityIndexBuilders);
    updateIndicesService.initializeAspectRetriever(aspectRetriever);

    return updateIndicesService;
  }

  @Bean
  @ConditionalOnProperty(name = "entityClient.impl", havingValue = "java", matchIfMissing = true)
  public UpdateIndicesService searchIndicesServiceGMS(
      final GraphService graphService,
      final EntitySearchService entitySearchService,
      final TimeseriesAspectService timeseriesAspectService,
      final SystemMetadataService systemMetadataService,
      final SearchDocumentTransformer searchDocumentTransformer,
      final EntityIndexBuilders entityIndexBuilders,
      final EntityService<?> entityService) {

    UpdateIndicesService updateIndicesService =
        new UpdateIndicesService(
            graphService,
            entitySearchService,
            timeseriesAspectService,
            systemMetadataService,
            searchDocumentTransformer,
            entityIndexBuilders);

    updateIndicesService.initializeAspectRetriever(entityService);
    entityService.setUpdateIndicesService(updateIndicesService);

    return updateIndicesService;
  }
}
