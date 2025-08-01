package com.linkedin.gms.factory.search;

import static com.linkedin.metadata.Constants.*;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.config.search.SearchConfiguration;
import com.linkedin.metadata.config.search.custom.CustomSearchConfiguration;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.search.elasticsearch.ElasticSearchService;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.SettingsBuilder;
import com.linkedin.metadata.search.elasticsearch.query.ESBrowseDAO;
import com.linkedin.metadata.search.elasticsearch.query.ESSearchDAO;
import com.linkedin.metadata.search.elasticsearch.query.filter.QueryFilterRewriteChain;
import com.linkedin.metadata.search.elasticsearch.update.ESWriteDAO;
import io.datahubproject.metadata.context.ObjectMapperContext;
import java.io.IOException;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Slf4j
@Configuration
public class ElasticSearchServiceFactory {

  @Autowired
  @Qualifier("baseElasticSearchComponents")
  private BaseElasticSearchComponentsFactory.BaseElasticSearchComponents components;

  @Autowired
  @Qualifier("settingsBuilder")
  private SettingsBuilder settingsBuilder;

  @Autowired
  @Qualifier("entityRegistry")
  private EntityRegistry entityRegistry;

  @Bean
  protected ElasticSearchConfiguration elasticSearchConfiguration(
      final ConfigurationProvider configurationProvider) {
    log.info("Search configuration: {}", configurationProvider.getElasticSearch().getSearch());
    return configurationProvider.getElasticSearch();
  }

  @Bean
  @Nullable
  protected CustomSearchConfiguration customSearchConfiguration(
      final ElasticSearchConfiguration elasticSearchConfiguration) throws IOException {
    SearchConfiguration searchConfiguration = elasticSearchConfiguration.getSearch();
    return searchConfiguration.getCustom() == null
        ? null
        : searchConfiguration.getCustom().resolve(ObjectMapperContext.DEFAULT.getYamlMapper());
  }

  @Bean
  protected ESSearchDAO esSearchDAO(
      final ConfigurationProvider configurationProvider,
      final QueryFilterRewriteChain queryFilterRewriteChain,
      final ElasticSearchConfiguration elasticSearchConfiguration,
      @Nullable final CustomSearchConfiguration customSearchConfiguration) {

    return new ESSearchDAO(
        components.getSearchClient(),
        configurationProvider.getFeatureFlags().isPointInTimeCreationEnabled(),
        elasticSearchConfiguration.getImplementation(),
        elasticSearchConfiguration,
        customSearchConfiguration,
        queryFilterRewriteChain,
        configurationProvider.getSearchService());
  }

  @Bean
  protected ESWriteDAO esWriteDAO() {
    return new ESWriteDAO(
        components.getConfig(), components.getSearchClient(), components.getBulkProcessor());
  }

  @Bean(name = "elasticSearchService")
  @Nonnull
  protected ElasticSearchService getInstance(
      final ConfigurationProvider configurationProvider,
      final QueryFilterRewriteChain queryFilterRewriteChain,
      final ElasticSearchConfiguration elasticSearchConfiguration,
      @Nullable final CustomSearchConfiguration customSearchConfiguration,
      final ESSearchDAO esSearchDAO,
      final ESWriteDAO esWriteDAO)
      throws IOException {

    return new ElasticSearchService(
        components.getIndexBuilder(),
        entityRegistry,
        components.getIndexConvention(),
        settingsBuilder,
        configurationProvider.getSearchService(),
        esSearchDAO,
        new ESBrowseDAO(
            components.getSearchClient(),
            elasticSearchConfiguration,
            customSearchConfiguration,
            queryFilterRewriteChain,
            configurationProvider.getSearchService()),
        esWriteDAO);
  }
}
