package com.linkedin.gms.factory.entity.update.indices;

import com.linkedin.gms.factory.common.IndexConventionFactory;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.gms.factory.search.ElasticSearchServiceFactory;
import com.linkedin.gms.factory.search.EntityIndexV3EnabledCondition;
import com.linkedin.metadata.config.postgres.PostgresSqlSetupProperties;
import com.linkedin.metadata.config.search.EntityIndexVersionConfiguration;
import com.linkedin.metadata.config.search.SemanticSearchConfiguration;
import com.linkedin.metadata.config.search.TimeseriesWriteThrottleConfiguration;
import com.linkedin.metadata.search.elasticsearch.ElasticSearchService;
import com.linkedin.metadata.search.elasticsearch.index.entity.v2.V2MappingsBuilder;
import com.linkedin.metadata.search.transformer.SearchDocumentTransformer;
import com.linkedin.metadata.search.write.EntitySearchWriteSink;
import com.linkedin.metadata.service.PostgresEntitySearchStrategy;
import com.linkedin.metadata.service.TimeseriesWriteThrottleCache;
import com.linkedin.metadata.service.UpdateIndicesStrategy;
import com.linkedin.metadata.service.UpdateIndicesV2Strategy;
import com.linkedin.metadata.service.UpdateIndicesV3Strategy;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.metadata.timeseries.write.TimeseriesAspectWriteSink;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration
@Import({ElasticSearchServiceFactory.class})
@Slf4j
public class UpdateIndicesStrategyFactory {

  @Bean
  @Nonnull
  protected TimeseriesWriteThrottleCache timeseriesWriteThrottleCache(
      ConfigurationProvider configProvider) {
    TimeseriesWriteThrottleConfiguration throttleConfig =
        configProvider.getMetadataChangeLog() != null
                && configProvider.getMetadataChangeLog().getThrottle() != null
            ? configProvider.getMetadataChangeLog().getThrottle().getTimeseries()
            : null;
    if (throttleConfig == null) {
      throttleConfig = TimeseriesWriteThrottleConfiguration.builder().build();
    }
    return new TimeseriesWriteThrottleCache(throttleConfig);
  }

  @Bean("updateIndicesV2Strategy")
  @ConditionalOnProperty(
      prefix = "elasticsearch",
      name = "enabled",
      havingValue = "true",
      matchIfMissing = true)
  @ConditionalOnProperty(name = "elasticsearch.entityIndex.v2.enabled", havingValue = "true")
  @Nonnull
  protected UpdateIndicesStrategy createUpdateIndicesV2Strategy(
      ElasticSearchService elasticSearchService,
      SearchDocumentTransformer searchDocumentTransformer,
      TimeseriesAspectService timeseriesAspectService,
      TimeseriesAspectWriteSink timeseriesAspectWriteSink,
      ConfigurationProvider configProvider,
      @Qualifier(IndexConventionFactory.INDEX_CONVENTION_BEAN) IndexConvention indexConvention,
      @Qualifier("legacyMappingsBuilder") V2MappingsBuilder mappingsBuilder,
      TimeseriesWriteThrottleCache timeseriesWriteThrottleCache,
      @Value("${elasticsearch.idHashAlgo}") String idHashAlgo,
      @Value("${elasticsearch.entityIndex.v2.cleanup:false}") boolean v2Cleanup,
      @Value("${elasticsearch.entityIndex.v2.coalesceBatchUpdates:false}")
          boolean coalesceBatchUpdates) {

    EntityIndexVersionConfiguration v2Config =
        EntityIndexVersionConfiguration.builder().enabled(true).cleanup(v2Cleanup).build();

    // Get semantic search configuration for dual-write support
    SemanticSearchConfiguration semanticSearchConfig =
        configProvider.getElasticSearch().getEntityIndex().getSemanticSearch();

    log.info(
        "Creating UpdateIndicesV2Strategy bean with semantic search enabled: {}, entities: {}",
        semanticSearchConfig != null && semanticSearchConfig.isEnabled(),
        semanticSearchConfig != null ? semanticSearchConfig.getEnabledEntities() : "none");

    return new UpdateIndicesV2Strategy(
        v2Config,
        elasticSearchService,
        searchDocumentTransformer,
        timeseriesAspectService,
        timeseriesAspectWriteSink,
        idHashAlgo,
        semanticSearchConfig,
        indexConvention,
        coalesceBatchUpdates,
        mappingsBuilder,
        timeseriesWriteThrottleCache);
  }

  @Bean("updateIndicesV3Strategy")
  @ConditionalOnProperty(
      prefix = "elasticsearch",
      name = "enabled",
      havingValue = "true",
      matchIfMissing = true)
  @Conditional(EntityIndexV3EnabledCondition.class)
  @Nonnull
  protected UpdateIndicesStrategy createUpdateIndicesV3Strategy(
      ElasticSearchService elasticSearchService,
      SearchDocumentTransformer searchDocumentTransformer,
      TimeseriesAspectService timeseriesAspectService,
      TimeseriesAspectWriteSink timeseriesAspectWriteSink,
      @Value("${elasticsearch.idHashAlgo}") String idHashAlgo,
      @Value("${elasticsearch.entityIndex.v3.cleanup:false}") boolean v3Cleanup,
      @Value("${elasticsearch.entityIndex.v2.enabled:true}") boolean v2Enabled) {

    EntityIndexVersionConfiguration v3Config =
        EntityIndexVersionConfiguration.builder().enabled(true).cleanup(v3Cleanup).build();

    log.info(
        "Creating UpdateIndicesV3Strategy bean (v2.enabled={}, v3 owns timeseries writes when v2 is off)",
        v2Enabled);
    return new UpdateIndicesV3Strategy(
        v3Config,
        elasticSearchService,
        searchDocumentTransformer,
        timeseriesAspectService,
        timeseriesAspectWriteSink,
        idHashAlgo,
        v2Enabled);
  }

  @Bean("postgresEntitySearchStrategy")
  @ConditionalOnProperty(
      name = "postgres.pgSearch.entity.enabled",
      havingValue = "true",
      matchIfMissing = false)
  @Nonnull
  protected UpdateIndicesStrategy createPostgresEntitySearchStrategy(
      PostgresSqlSetupProperties postgresSqlSetupProperties,
      EntitySearchWriteSink entitySearchWriteSink,
      SearchDocumentTransformer searchDocumentTransformer) {

    log.info("Creating PostgresEntitySearchStrategy (combined-document pgSearch path)");

    return new PostgresEntitySearchStrategy(
        postgresSqlSetupProperties, entitySearchWriteSink, searchDocumentTransformer);
  }
}
