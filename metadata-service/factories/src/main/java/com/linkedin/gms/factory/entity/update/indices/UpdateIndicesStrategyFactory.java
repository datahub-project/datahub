package com.linkedin.gms.factory.entity.update.indices;

import com.linkedin.gms.factory.common.IndexConventionFactory;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.gms.factory.search.ElasticSearchServiceFactory;
import com.linkedin.gms.factory.search.EntityDocumentIdHasherFactory;
import com.linkedin.gms.factory.search.SearchClusterRegistry;
import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.config.search.EntityIndexVersionConfiguration;
import com.linkedin.metadata.config.search.IndexConfiguration;
import com.linkedin.metadata.config.search.SearchComponent;
import com.linkedin.metadata.config.search.SearchConfiguration;
import com.linkedin.metadata.config.search.SearchV3Configuration;
import com.linkedin.metadata.config.search.SemanticSearchConfiguration;
import com.linkedin.metadata.config.search.TimeseriesWriteThrottleConfiguration;
import com.linkedin.metadata.search.elasticsearch.ElasticSearchService;
import com.linkedin.metadata.search.elasticsearch.index.StructuredPropertyIndexFields;
import com.linkedin.metadata.search.elasticsearch.index.StructuredPropertyMappingCache;
import com.linkedin.metadata.search.elasticsearch.index.entity.v2.V2MappingsBuilder;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.EntityDocumentIdHasher;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.UpdateIndicesV3Strategy;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.V3SearchDocumentContributor;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.V3SearchIndexSkip;
import com.linkedin.metadata.search.transformer.SearchDocumentTransformer;
import com.linkedin.metadata.service.IncrementalReindexWriteTargets;
import com.linkedin.metadata.service.TimeseriesWriteThrottleCache;
import com.linkedin.metadata.service.UpdateIndicesStrategy;
import com.linkedin.metadata.service.UpdateIndicesV2Strategy;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.opensearch.client.RequestOptions;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration
@Import({ElasticSearchServiceFactory.class, EntityDocumentIdHasherFactory.class})
@Slf4j
public class UpdateIndicesStrategyFactory {

  private static final String EBEAN_ENTITY_SERVICE_IMPL = "ebean";

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
  @ConditionalOnProperty(name = "elasticsearch.entityIndex.v2.enabled", havingValue = "true")
  @Nonnull
  protected UpdateIndicesStrategy createUpdateIndicesV2Strategy(
      ElasticSearchService elasticSearchService,
      SearchDocumentTransformer searchDocumentTransformer,
      TimeseriesAspectService timeseriesAspectService,
      ConfigurationProvider configProvider,
      @Qualifier(IndexConventionFactory.INDEX_CONVENTION_BEAN) IndexConvention indexConvention,
      @Qualifier("legacyMappingsBuilder") V2MappingsBuilder mappingsBuilder,
      TimeseriesWriteThrottleCache timeseriesWriteThrottleCache,
      @Value("${elasticsearch.entityIndex.v2.idHashAlgo}") String idHashAlgo,
      @Value("${elasticsearch.entityIndex.v2.cleanup:false}") boolean v2Cleanup,
      @Value("${elasticsearch.entityIndex.v2.coalesceBatchUpdates:false}")
          boolean coalesceBatchUpdates,
      StructuredPropertyMappingCache structuredPropertyMappingCache) {

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
        idHashAlgo,
        semanticSearchConfig,
        indexConvention,
        coalesceBatchUpdates,
        mappingsBuilder,
        timeseriesWriteThrottleCache,
        structuredPropertyMappingCache);
  }

  @Bean("updateIndicesV3Strategy")
  @ConditionalOnProperty(name = "elasticsearch.entityIndex.v3.enabled", havingValue = "true")
  @Nonnull
  protected UpdateIndicesStrategy createUpdateIndicesV3Strategy(
      ElasticSearchService elasticSearchService,
      SearchDocumentTransformer searchDocumentTransformer,
      TimeseriesAspectService timeseriesAspectService,
      TimeseriesWriteThrottleCache timeseriesWriteThrottleCache,
      ConfigurationProvider configProvider,
      EntityDocumentIdHasher entityDocumentIdHasher,
      @Autowired(required = false) @Nullable List<V3SearchDocumentContributor> documentContributors,
      @Value("${elasticsearch.entityIndex.v3.cleanup:false}") boolean v3Cleanup,
      @Value("${elasticsearch.entityIndex.v2.enabled:true}") boolean v2Enabled,
      @Value("${elasticsearch.entityIndex.v2.idHashAlgo}") String idHashAlgo,
      @Value("${entityService.impl:ebean}") String entityServiceImpl,
      final SearchClusterRegistry searchClusterRegistry,
      @Qualifier("incrementalReindexWriteTargets") @Nullable
          IncrementalReindexWriteTargets incrementalReindexWriteTargets,
      StructuredPropertyMappingCache structuredPropertyMappingCache,
      @Autowired(required = false) @Nullable List<V3SearchIndexSkip> searchIndexSkips) {

    validateV3WritePrimaryStorage(entityServiceImpl);

    EntityIndexVersionConfiguration v3Config =
        EntityIndexVersionConfiguration.builder().enabled(true).cleanup(v3Cleanup).build();
    boolean vectorProjectionEnabled = isV3VectorProjectionEnabled(configProvider);

    log.info(
        "Creating UpdateIndicesV3Strategy bean with vectorProjectionEnabled={}",
        vectorProjectionEnabled);
    return UpdateIndicesV3Strategy.builder()
        .v3Config(v3Config)
        .elasticSearchService(elasticSearchService)
        .searchDocumentTransformer(searchDocumentTransformer)
        .timeseriesAspectService(timeseriesAspectService)
        .v2Enabled(v2Enabled)
        .idHashAlgo(idHashAlgo)
        .timeseriesThrottleCache(timeseriesWriteThrottleCache)
        .searchClientShim(searchClusterRegistry.clientFor(SearchComponent.SEARCH_V3))
        .vectorProjectionEnabled(vectorProjectionEnabled)
        .expectedVectorDimensions(expectedVectorDimensions(configProvider))
        .entityDocumentIdHasher(entityDocumentIdHasher)
        .documentContributors(documentContributors)
        .semanticSearchConfiguration(semanticSearchConfiguration(configProvider))
        .incrementalReindexWriteTargets(incrementalReindexWriteTargets)
        .structuredPropertyMappingCache(structuredPropertyMappingCache)
        .searchIndexSkips(searchIndexSkips)
        .build();
  }

  static boolean isV3VectorProjectionEnabled(@Nullable final ConfigurationProvider configProvider) {
    if (configProvider == null) {
      return false;
    }
    final ElasticSearchConfiguration elasticSearch = configProvider.getElasticSearch();
    if (elasticSearch == null) {
      return false;
    }
    final SearchConfiguration search = elasticSearch.getSearch();
    if (search == null) {
      return false;
    }
    final SearchV3Configuration v3 = search.getV3();
    return v3 != null && v3.isVectorProjectionEnabled();
  }

  @Nullable
  private static SemanticSearchConfiguration semanticSearchConfiguration(
      @Nullable final ConfigurationProvider configProvider) {
    if (configProvider == null
        || configProvider.getElasticSearch() == null
        || configProvider.getElasticSearch().getEntityIndex() == null) {
      return null;
    }
    return configProvider.getElasticSearch().getEntityIndex().getSemanticSearch();
  }

  @Nonnull
  private static Map<String, Integer> expectedVectorDimensions(
      @Nullable final ConfigurationProvider configProvider) {
    if (configProvider == null
        || configProvider.getElasticSearch() == null
        || configProvider.getElasticSearch().getEntityIndex() == null
        || configProvider.getElasticSearch().getEntityIndex().getSemanticSearch() == null
        || configProvider.getElasticSearch().getEntityIndex().getSemanticSearch().getModels()
            == null) {
      return Map.of();
    }
    final Map<String, Integer> dimensions = new LinkedHashMap<>();
    configProvider
        .getElasticSearch()
        .getEntityIndex()
        .getSemanticSearch()
        .getModels()
        .forEach(
            (modelKey, modelConfig) -> {
              if (modelConfig != null) {
                dimensions.put(modelKey, modelConfig.getVectorDimension());
              }
            });
    return dimensions;
  }

  static void validateV3WritePrimaryStorage(@Nullable final String entityServiceImpl) {
    final String normalized =
        entityServiceImpl == null ? EBEAN_ENTITY_SERVICE_IMPL : entityServiceImpl.trim();
    if (normalized.isEmpty() || EBEAN_ENTITY_SERVICE_IMPL.equalsIgnoreCase(normalized)) {
      return;
    }
    throw new IllegalStateException(
        "Search V3 writes currently require entityService.impl=ebean. "
            + "CassandraAspectDao.streamAspectBatches returns an empty stream, so a V3 fill "
            + "and catch-up would index nothing. Disable elasticsearch.entityIndex.v3.enabled "
            + "before using entityService.impl="
            + normalized
            + ".");
  }

  /**
   * One mapping read per index per TTL, shared by the index writers and the on-demand consistency
   * check. The flag defaults on; a missing index configuration keeps that default.
   */
  @Bean
  @Nonnull
  public StructuredPropertyMappingCache structuredPropertyMappingCache(
      @Nonnull ConfigurationProvider configProvider,
      @Nonnull ElasticSearchService elasticSearchService) {
    IndexConfiguration index =
        configProvider.getElasticSearch() == null
            ? null
            : configProvider.getElasticSearch().getIndex();
    boolean enabled = index == null || index.isStructuredPropertyRestateEnabled();
    int ttlSeconds = index == null ? 300 : index.getStructuredPropertyMappingCacheTtlSeconds();
    return new StructuredPropertyMappingCache(
        enabled, ttlSeconds, elasticSearchService::indexMappingSource);
  }

  /** Reads structured-property values a batch at a time from the index being compared. */
  @Bean
  @Nonnull
  public StructuredPropertyIndexFields structuredPropertyIndexFields(
      @Nonnull ElasticSearchService elasticSearchService) {
    return new StructuredPropertyIndexFields(
        (opContext, indexName, request) ->
            elasticSearchService
                .getIndexBuilder(indexName)
                .getSearchClient()
                .search(opContext, request, RequestOptions.DEFAULT));
  }
}
