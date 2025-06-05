package com.linkedin.gms.factory.search;

import static com.linkedin.metadata.Constants.*;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.gms.factory.entityregistry.EntityRegistryFactory;
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
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Slf4j
@Configuration
@Import({EntityRegistryFactory.class, SettingsBuilderFactory.class})
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

  @Bean(name = "elasticSearchService")
  @Nonnull
  protected ElasticSearchService getInstance(
      final ConfigurationProvider configurationProvider,
      final QueryFilterRewriteChain queryFilterRewriteChain)
      throws IOException {
    log.info("Search configuration: {}", configurationProvider.getElasticSearch().getSearch());

    ElasticSearchConfiguration elasticSearchConfiguration =
        configurationProvider.getElasticSearch();
    SearchConfiguration searchConfiguration = elasticSearchConfiguration.getSearch();
    CustomSearchConfiguration customSearchConfiguration =
        searchConfiguration.getCustom() == null
            ? null
            : searchConfiguration.getCustom().resolve(ObjectMapperContext.DEFAULT.getYamlMapper());

    ESSearchDAO esSearchDAO =
        new ESSearchDAO(
            components.getSearchClient(),
            configurationProvider.getFeatureFlags().isPointInTimeCreationEnabled(),
            elasticSearchConfiguration.getImplementation(),
            elasticSearchConfiguration,
            customSearchConfiguration,
            queryFilterRewriteChain);
    return new ElasticSearchService(
        components.getIndexBuilder(),
        entityRegistry,
        components.getIndexConvention(),
        settingsBuilder,
        elasticSearchConfiguration,
        esSearchDAO,
        new ESBrowseDAO(
            components.getSearchClient(),
            elasticSearchConfiguration,
            customSearchConfiguration,
            queryFilterRewriteChain),
        new ESWriteDAO(
            components.getSearchClient(),
            components.getBulkProcessor(),
            components.getNumRetries()));
  }
}
