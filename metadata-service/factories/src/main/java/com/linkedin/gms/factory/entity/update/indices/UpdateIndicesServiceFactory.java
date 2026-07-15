package com.linkedin.gms.factory.entity.update.indices;

import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.graph.GraphService;
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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Slf4j
@Configuration
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
      @Qualifier("updateIndicesV3Strategy") @Nullable UpdateIndicesStrategy v3Strategy,
      @Qualifier("updateIndicesUpgradeStrategy") @Nullable UpdateIndicesStrategy upgradeStrategy,
      @Qualifier("postgresEntitySearchStrategy") @Nullable
          UpdateIndicesStrategy postgresEntitySearchStrategy) {

    Collection<UpdateIndicesStrategy> strategies = new ArrayList<>();

    if (v2Strategy != null) {
      strategies.add(v2Strategy);
    }

    if (v3Strategy != null) {
      strategies.add(v3Strategy);
    }

    if (upgradeStrategy != null) {
      strategies.add(upgradeStrategy);
    }

    if (postgresEntitySearchStrategy != null) {
      strategies.add(postgresEntitySearchStrategy);
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
      TimeseriesAspectService timeseriesAspectService,
      SystemMetadataService systemMetadataService,
      SearchDocumentTransformer searchDocumentTransformer,
      @Value("${elasticsearch.idHashAlgo}") final String idHashAlgo,
      @Value("#{'${featureFlags.fineGrainedLineageNotAllowedForPlatforms}'.split(',')}")
          final List<String> fineGrainedLineageNotAllowedForPlatforms,
      @Qualifier("updateIndicesV2Strategy") @Nullable UpdateIndicesStrategy v2Strategy,
      @Qualifier("updateIndicesV3Strategy") @Nullable UpdateIndicesStrategy v3Strategy,
      @Qualifier("updateIndicesUpgradeStrategy") @Nullable UpdateIndicesStrategy upgradeStrategy,
      @Autowired(required = false) @Qualifier("postgresEntitySearchStrategy") @Nullable
          UpdateIndicesStrategy postgresEntitySearchStrategy) {

    Collection<UpdateIndicesStrategy> strategies =
        createStrategies(v2Strategy, v3Strategy, upgradeStrategy, postgresEntitySearchStrategy);

    return new UpdateIndicesService(
        new UpdateGraphIndicesService(
            graphService,
            graphDiffMode,
            graphStatusEnabled,
            fineGrainedLineageNotAllowedForPlatforms),
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
      final TimeseriesAspectService timeseriesAspectService,
      final SystemMetadataService systemMetadataService,
      final SearchDocumentTransformer searchDocumentTransformer,
      final EntityService<?> entityService,
      @Value("${elasticsearch.idHashAlgo}") final String idHashAlgo,
      @Value("#{'${featureFlags.fineGrainedLineageNotAllowedForPlatforms}'.split(',')}")
          final List<String> fineGrainedLineageNotAllowedForPlatforms,
      @Qualifier("updateIndicesV2Strategy") @Nullable UpdateIndicesStrategy v2Strategy,
      @Qualifier("updateIndicesV3Strategy") @Nullable UpdateIndicesStrategy v3Strategy,
      @Qualifier("updateIndicesUpgradeStrategy") @Nullable UpdateIndicesStrategy upgradeStrategy,
      @Autowired(required = false) @Qualifier("postgresEntitySearchStrategy") @Nullable
          UpdateIndicesStrategy postgresEntitySearchStrategy) {

    Collection<UpdateIndicesStrategy> strategies =
        createStrategies(v2Strategy, v3Strategy, upgradeStrategy, postgresEntitySearchStrategy);

    UpdateIndicesService updateIndicesService =
        new UpdateIndicesService(
            new UpdateGraphIndicesService(
                graphService,
                graphDiffMode,
                graphStatusEnabled,
                fineGrainedLineageNotAllowedForPlatforms),
            systemMetadataService,
            strategies,
            searchDiffMode,
            structuredPropertiesHookEnabled,
            structuredPropertiesWriteEnabled);

    entityService.setUpdateIndicesService(updateIndicesService);

    return updateIndicesService;
  }
}
