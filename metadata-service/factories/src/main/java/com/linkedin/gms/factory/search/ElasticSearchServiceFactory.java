package com.linkedin.gms.factory.search;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.gms.factory.entityregistry.EntityRegistryFactory;
import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.config.search.SearchConfiguration;
import com.linkedin.metadata.config.search.custom.CustomSearchConfiguration;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.search.elasticsearch.ElasticSearchService;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.EntityIndexBuilders;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.SettingsBuilder;
import com.linkedin.metadata.search.elasticsearch.query.ESBrowseDAO;
import com.linkedin.metadata.search.elasticsearch.query.ESSearchDAO;
import com.linkedin.metadata.search.elasticsearch.update.ESWriteDAO;
import com.linkedin.metadata.spring.YamlPropertySourceFactory;
import java.io.IOException;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.PropertySource;

@Slf4j
@Configuration
@PropertySource(value = "classpath:/application.yml", factory = YamlPropertySourceFactory.class)
@Import({EntityRegistryFactory.class, SettingsBuilderFactory.class})
public class ElasticSearchServiceFactory {
  private static final ObjectMapper YAML_MAPPER = new YAMLMapper();

  @Autowired
  @Qualifier("baseElasticSearchComponents")
  private BaseElasticSearchComponentsFactory.BaseElasticSearchComponents components;

  @Autowired
  @Qualifier("entityRegistry")
  private EntityRegistry entityRegistry;

  @Autowired
  @Qualifier("settingsBuilder")
  private SettingsBuilder settingsBuilder;

  @Autowired private EntityIndexBuilders entityIndexBuilders;

  @Autowired private ConfigurationProvider configurationProvider;

  @Bean(name = "elasticSearchService")
  @Nonnull
  protected ElasticSearchService getInstance(ConfigurationProvider configurationProvider)
      throws IOException {
    log.info("Search configuration: {}", configurationProvider.getElasticSearch().getSearch());

    ElasticSearchConfiguration elasticSearchConfiguration =
        configurationProvider.getElasticSearch();
    SearchConfiguration searchConfiguration = elasticSearchConfiguration.getSearch();
    CustomSearchConfiguration customSearchConfiguration =
        searchConfiguration.getCustom() == null
            ? null
            : searchConfiguration.getCustom().resolve(YAML_MAPPER);

    ESSearchDAO esSearchDAO =
        new ESSearchDAO(
            entityRegistry,
            components.getSearchClient(),
            components.getIndexConvention(),
            configurationProvider.getFeatureFlags().isPointInTimeCreationEnabled(),
            elasticSearchConfiguration.getImplementation(),
            searchConfiguration,
            customSearchConfiguration);
    return new ElasticSearchService(
        entityIndexBuilders,
        esSearchDAO,
        new ESBrowseDAO(
            entityRegistry,
            components.getSearchClient(),
            components.getIndexConvention(),
            searchConfiguration,
            customSearchConfiguration),
        new ESWriteDAO(
            entityRegistry,
            components.getSearchClient(),
            components.getIndexConvention(),
            components.getBulkProcessor(),
            components.getNumRetries()));
  }
}
