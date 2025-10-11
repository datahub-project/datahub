package com.linkedin.gms.factory.entity.update.indices;

import com.linkedin.gms.factory.search.ElasticSearchServiceFactory;
import com.linkedin.metadata.config.search.EntityIndexVersionConfiguration;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.search.elasticsearch.ElasticSearchService;
import com.linkedin.metadata.search.transformer.SearchDocumentTransformer;
import com.linkedin.metadata.service.UpdateGraphIndicesService;
import com.linkedin.metadata.service.UpdateIndicesService;
import com.linkedin.metadata.service.UpdateIndicesStrategy;
import com.linkedin.metadata.service.UpdateIndicesV2Strategy;
import com.linkedin.metadata.service.UpdateIndicesV3Strategy;
import com.linkedin.metadata.systemmetadata.SystemMetadataService;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
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

  @Value("${elasticsearch.entityIndex.v2.enabled:true}")
  private boolean v2Enabled;

  @Value("${elasticsearch.entityIndex.v2.cleanup:false}")
  private boolean v2Cleanup;

  @Value("${elasticsearch.entityIndex.v3.enabled:true}")
  private boolean v3Enabled;

  @Value("${elasticsearch.entityIndex.v3.cleanup:false}")
  private boolean v3Cleanup;

  /** Creates a collection of UpdateIndicesStrategy instances based on configuration. */
  private Collection<UpdateIndicesStrategy> createStrategies(
      ElasticSearchService elasticSearchService,
      SearchDocumentTransformer searchDocumentTransformer,
      TimeseriesAspectService timeseriesAspectService,
      String idHashAlgo) {

    Collection<UpdateIndicesStrategy> strategies = new ArrayList<>();

    // Create V2 strategy if enabled
    if (v2Enabled) {
      EntityIndexVersionConfiguration v2Config =
          EntityIndexVersionConfiguration.builder().enabled(true).cleanup(v2Cleanup).build();
      strategies.add(
          new UpdateIndicesV2Strategy(
              v2Config,
              elasticSearchService,
              searchDocumentTransformer,
              timeseriesAspectService,
              idHashAlgo));
    }

    // Create V3 strategy if enabled
    if (v3Enabled) {
      EntityIndexVersionConfiguration v3Config =
          EntityIndexVersionConfiguration.builder().enabled(true).cleanup(v3Cleanup).build();
      strategies.add(
          new UpdateIndicesV3Strategy(
              v3Config,
              elasticSearchService,
              searchDocumentTransformer,
              timeseriesAspectService,
              idHashAlgo,
              v2Enabled)); // Pass v2Enabled flag to avoid duplication
    }

    return strategies;
  }

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
      @Value("${elasticsearch.idHashAlgo}") final String idHashAlgo,
      @Value("#{'${featureFlags.fineGrainedLineageNotAllowedForPlatforms}'.split(',')}")
          final List<String> fineGrainedLineageNotAllowedForPlatforms) {

    Collection<UpdateIndicesStrategy> strategies =
        createStrategies(
            entitySearchService, searchDocumentTransformer, timeseriesAspectService, idHashAlgo);

    return new UpdateIndicesService(
        new UpdateGraphIndicesService(
            graphService,
            graphDiffMode,
            graphStatusEnabled,
            fineGrainedLineageNotAllowedForPlatforms),
        entitySearchService,
        systemMetadataService,
        strategies,
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
      @Value("${elasticsearch.idHashAlgo}") final String idHashAlgo,
      @Value("#{'${featureFlags.fineGrainedLineageNotAllowedForPlatforms}'.split(',')}")
          final List<String> fineGrainedLineageNotAllowedForPlatforms) {

    Collection<UpdateIndicesStrategy> strategies =
        createStrategies(
            entitySearchService, searchDocumentTransformer, timeseriesAspectService, idHashAlgo);

    UpdateIndicesService updateIndicesService =
        new UpdateIndicesService(
            new UpdateGraphIndicesService(
                graphService,
                graphDiffMode,
                graphStatusEnabled,
                fineGrainedLineageNotAllowedForPlatforms),
            entitySearchService,
            systemMetadataService,
            strategies,
            searchDiffMode,
            structuredPropertiesHookEnabled,
            structuredPropertiesWriteEnabled);

    entityService.setUpdateIndicesService(updateIndicesService);

    return updateIndicesService;
  }
}
