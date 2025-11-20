package com.linkedin.gms.factory.entity.update.indices;

import com.linkedin.gms.factory.search.ElasticSearchServiceFactory;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.search.elasticsearch.ElasticSearchService;
import com.linkedin.metadata.search.transformer.SearchDocumentTransformer;
import com.linkedin.metadata.service.UpdateGraphIndicesService;
import com.linkedin.metadata.service.UpdateIndicesService;
import com.linkedin.metadata.service.UpdateIndicesStrategy;
import com.linkedin.metadata.systemmetadata.SystemMetadataService;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Slf4j
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

  /** Creates a collection of UpdateIndicesStrategy instances based on Spring beans. */
  private Collection<UpdateIndicesStrategy> createStrategies(
      @Qualifier("updateIndicesV2Strategy") @Nullable UpdateIndicesStrategy v2Strategy,
      @Qualifier("updateIndicesV3Strategy") @Nullable UpdateIndicesStrategy v3Strategy) {

    Collection<UpdateIndicesStrategy> strategies = new ArrayList<>();

    if (v2Strategy != null) {
      strategies.add(v2Strategy);
    }

    if (v3Strategy != null) {
      strategies.add(v3Strategy);
    }

    List<String> strategyNames =
        strategies.stream().map(strategy -> strategy.getClass().getSimpleName()).toList();

    log.info(
        "UpdateIndicesService strategies: {}",
        strategyNames.isEmpty() ? "None" : String.join(", ", strategyNames));

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
          final List<String> fineGrainedLineageNotAllowedForPlatforms,
      @Qualifier("updateIndicesV2Strategy") @Nullable UpdateIndicesStrategy v2Strategy,
      @Qualifier("updateIndicesV3Strategy") @Nullable UpdateIndicesStrategy v3Strategy) {

    Collection<UpdateIndicesStrategy> strategies = createStrategies(v2Strategy, v3Strategy);

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
          final List<String> fineGrainedLineageNotAllowedForPlatforms,
      @Qualifier("updateIndicesV2Strategy") @Nullable UpdateIndicesStrategy v2Strategy,
      @Qualifier("updateIndicesV3Strategy") @Nullable UpdateIndicesStrategy v3Strategy) {

    Collection<UpdateIndicesStrategy> strategies = createStrategies(v2Strategy, v3Strategy);

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
