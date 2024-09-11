package com.linkedin.gms.factory.search;

import static com.linkedin.metadata.Constants.*;

import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.gms.factory.entityregistry.EntityRegistryFactory;
import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.config.search.SearchConfiguration;
import com.linkedin.metadata.config.search.custom.CustomSearchConfiguration;
import com.linkedin.metadata.search.elasticsearch.ElasticSearchService;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.EntityIndexBuilders;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.SettingsBuilder;
import com.linkedin.metadata.search.elasticsearch.query.ESBrowseDAO;
import com.linkedin.metadata.search.elasticsearch.query.ESSearchDAO;
import com.linkedin.metadata.search.elasticsearch.query.filter.QueryFilterRewriteChain;
import com.linkedin.metadata.search.elasticsearch.update.ESWriteDAO;
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
  private static final ObjectMapper YAML_MAPPER = new YAMLMapper();

  static {
    int maxSize =
        Integer.parseInt(
            System.getenv()
                .getOrDefault(INGESTION_MAX_SERIALIZED_STRING_LENGTH, MAX_JACKSON_STRING_SIZE));
    YAML_MAPPER
        .getFactory()
        .setStreamReadConstraints(StreamReadConstraints.builder().maxStringLength(maxSize).build());
  }

  @Autowired
  @Qualifier("baseElasticSearchComponents")
  private BaseElasticSearchComponentsFactory.BaseElasticSearchComponents components;

  @Autowired
  @Qualifier("settingsBuilder")
  private SettingsBuilder settingsBuilder;

  @Autowired private EntityIndexBuilders entityIndexBuilders;

  @Autowired private ConfigurationProvider configurationProvider;

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
            : searchConfiguration.getCustom().resolve(YAML_MAPPER);

    ESSearchDAO esSearchDAO =
        new ESSearchDAO(
            components.getSearchClient(),
            configurationProvider.getFeatureFlags().isPointInTimeCreationEnabled(),
            elasticSearchConfiguration.getImplementation(),
            searchConfiguration,
            customSearchConfiguration,
            queryFilterRewriteChain);
    return new ElasticSearchService(
        entityIndexBuilders,
        esSearchDAO,
        new ESBrowseDAO(
            components.getSearchClient(),
            searchConfiguration,
            customSearchConfiguration,
            queryFilterRewriteChain),
        new ESWriteDAO(
            components.getSearchClient(),
            components.getBulkProcessor(),
            components.getNumRetries()));
  }
}
