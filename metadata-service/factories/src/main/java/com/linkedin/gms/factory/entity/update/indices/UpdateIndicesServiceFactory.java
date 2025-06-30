package com.linkedin.gms.factory.entity.update.indices;

import com.linkedin.gms.factory.search.ElasticSearchServiceFactory;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.search.elasticsearch.ElasticSearchService;
import com.linkedin.metadata.search.transformer.SearchDocumentTransformer;
import com.linkedin.metadata.service.UpdateGraphIndicesService;
import com.linkedin.metadata.service.UpdateIndicesService;
import com.linkedin.metadata.systemmetadata.SystemMetadataService;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration
@Import(ElasticSearchServiceFactory.class)
public class UpdateIndicesServiceFactory {

  @Value("${featureFlags.searchServiceDiffModeEnabled}")
  private boolean searchDiffMode;

  @Value("${structuredProperties.enabled}")
  private boolean structuredPropertiesHookEnabled;

  @Value("${structuredProperties.writeEnabled}")
  private boolean structuredPropertiesWriteEnabled;

  @Value("${featureFlags.graphServiceDiffModeEnabled}")
  private boolean graphDiffMode;

  @Value("${elasticsearch.search.graph.graphStatusEnabled}")
  private boolean graphStatusEnabled;

  /*
   When restli mode the EntityService is not available. Wire in an AspectRetriever here instead
   based on the entity client
  */
  @Bean
  @ConditionalOnProperty(name = "entityClient.impl", havingValue = "restli")
  public UpdateIndicesService searchIndicesServiceNonGMS(
      GraphService graphService,
      ElasticSearchService entitySearchService,
      TimeseriesAspectService timeseriesAspectService,
      SystemMetadataService systemMetadataService,
      SearchDocumentTransformer searchDocumentTransformer,
      @Value("${elasticsearch.idHashAlgo}") final String idHashAlgo) {

    return new UpdateIndicesService(
        new UpdateGraphIndicesService(graphService, graphDiffMode, graphStatusEnabled),
        entitySearchService,
        timeseriesAspectService,
        systemMetadataService,
        searchDocumentTransformer,
        idHashAlgo,
        searchDiffMode,
        structuredPropertiesHookEnabled,
        structuredPropertiesWriteEnabled);
  }

  @Bean
  @ConditionalOnProperty(name = "entityClient.impl", havingValue = "java", matchIfMissing = true)
  public UpdateIndicesService searchIndicesServiceGMS(
      final GraphService graphService,
      final ElasticSearchService entitySearchService,
      final TimeseriesAspectService timeseriesAspectService,
      final SystemMetadataService systemMetadataService,
      final SearchDocumentTransformer searchDocumentTransformer,
      final EntityService<?> entityService,
      @Value("${elasticsearch.idHashAlgo}") final String idHashAlgo) {

    UpdateIndicesService updateIndicesService =
        new UpdateIndicesService(
            new UpdateGraphIndicesService(graphService, graphDiffMode, graphStatusEnabled),
            entitySearchService,
            timeseriesAspectService,
            systemMetadataService,
            searchDocumentTransformer,
            idHashAlgo,
            searchDiffMode,
            structuredPropertiesHookEnabled,
            structuredPropertiesWriteEnabled);

    entityService.setUpdateIndicesService(updateIndicesService);

    return updateIndicesService;
  }
}
