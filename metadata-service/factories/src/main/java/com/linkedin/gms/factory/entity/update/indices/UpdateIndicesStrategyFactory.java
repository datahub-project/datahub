package com.linkedin.gms.factory.entity.update.indices;

import com.linkedin.gms.factory.common.IndexConventionFactory;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.gms.factory.search.ElasticSearchServiceFactory;
import com.linkedin.metadata.config.search.EntityIndexVersionConfiguration;
import com.linkedin.metadata.config.search.SemanticSearchConfiguration;
import com.linkedin.metadata.search.elasticsearch.ElasticSearchService;
import com.linkedin.metadata.search.transformer.SearchDocumentTransformer;
import com.linkedin.metadata.service.UpdateIndicesStrategy;
import com.linkedin.metadata.service.UpdateIndicesV2Strategy;
import com.linkedin.metadata.service.UpdateIndicesV3Strategy;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration
@Import(ElasticSearchServiceFactory.class)
@Slf4j
public class UpdateIndicesStrategyFactory {

  @Bean("updateIndicesV2Strategy")
  @ConditionalOnProperty(name = "elasticsearch.entityIndex.v2.enabled", havingValue = "true")
  @Nonnull
  protected UpdateIndicesStrategy createUpdateIndicesV2Strategy(
      ElasticSearchService elasticSearchService,
      SearchDocumentTransformer searchDocumentTransformer,
      TimeseriesAspectService timeseriesAspectService,
      ConfigurationProvider configProvider,
      @Qualifier(IndexConventionFactory.INDEX_CONVENTION_BEAN) IndexConvention indexConvention,
      @Value("${elasticsearch.idHashAlgo}") String idHashAlgo,
      @Value("${elasticsearch.entityIndex.v2.cleanup:false}") boolean v2Cleanup) {

    EntityIndexVersionConfiguration v2Config =
        EntityIndexVersionConfiguration.builder().enabled(true).cleanup(v2Cleanup).build();

    // Get semantic search configuration for dual-write support
    SemanticSearchConfiguration semanticSearchConfig =
        configProvider.getElasticSearch().getEntityIndex().getSemanticSearch();

    // #region agent debug log - H2/H3/H4 instrumentation
    log.debug(
        "[DEBUG-DUALWRITE] SemanticSearchConfig null check: isNull={}",
        semanticSearchConfig == null);
    if (semanticSearchConfig != null) {
      log.debug(
          "[DEBUG-DUALWRITE] SemanticSearchConfig.isEnabled()={}",
          semanticSearchConfig.isEnabled());
      log.debug(
          "[DEBUG-DUALWRITE] SemanticSearchConfig.getEnabledEntities()={}",
          semanticSearchConfig.getEnabledEntities());
    }
    // #endregion
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
        indexConvention);
  }

  @Bean("updateIndicesV3Strategy")
  @ConditionalOnProperty(name = "elasticsearch.entityIndex.v3.enabled", havingValue = "true")
  @Nonnull
  protected UpdateIndicesStrategy createUpdateIndicesV3Strategy(
      ElasticSearchService elasticSearchService,
      SearchDocumentTransformer searchDocumentTransformer,
      TimeseriesAspectService timeseriesAspectService,
      @Value("${elasticsearch.idHashAlgo}") String idHashAlgo,
      @Value("${elasticsearch.entityIndex.v3.cleanup:false}") boolean v3Cleanup,
      @Value("${elasticsearch.entityIndex.v2.enabled:true}") boolean v2Enabled) {

    EntityIndexVersionConfiguration v3Config =
        EntityIndexVersionConfiguration.builder().enabled(true).cleanup(v3Cleanup).build();

    log.info("Creating UpdateIndicesV3Strategy bean");
    return new UpdateIndicesV3Strategy(
        v3Config,
        elasticSearchService,
        searchDocumentTransformer,
        timeseriesAspectService,
        idHashAlgo,
        v2Enabled);
  }
}
