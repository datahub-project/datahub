package com.linkedin.gms.factory.search;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.config.search.SearchComponent;
import com.linkedin.metadata.config.search.SearchConfiguration;
import com.linkedin.metadata.config.search.custom.CustomSearchConfiguration;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.search.elasticsearch.ElasticSearchService;
import com.linkedin.metadata.search.elasticsearch.index.MappingsBuilder;
import com.linkedin.metadata.search.elasticsearch.index.SettingsBuilder;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.EntityDocumentIdHasher;
import com.linkedin.metadata.search.elasticsearch.query.ESBrowseDAO;
import com.linkedin.metadata.search.elasticsearch.query.ESSearchDAO;
import com.linkedin.metadata.search.elasticsearch.query.filter.QueryFilterRewriteChain;
import com.linkedin.metadata.search.elasticsearch.update.ESWriteDAO;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import io.datahubproject.metadata.context.ObjectMapperContext;
import java.io.IOException;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Slf4j
@Configuration
@Import(EntityDocumentIdHasherFactory.class)
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
      @Nullable final CustomSearchConfiguration customSearchConfiguration,
      final EntityDocumentIdHasher entityDocumentIdHasher) {

    return new ESSearchDAO(
        elasticSearchConfiguration.getSearch().isPointInTimeCreationEnabled(),
        elasticSearchConfiguration,
        customSearchConfiguration,
        queryFilterRewriteChain,
        false,
        configurationProvider.getSearchService(),
        entityDocumentIdHasher);
  }

  @Bean
  protected ESWriteDAO esWriteDAO(
      final ConfigurationProvider configurationProvider,
      final SearchClusterRegistry searchClusterRegistry) {
    ESWriteDAO esWriteDAO =
        new ESWriteDAO(
            components.getConfig(),
            components.getSearchClient(),
            components.getBulkProcessor(),
            searchClusterRegistry);
    if (configurationProvider.getDatahub().isReadOnly()) {
      esWriteDAO.setWritable(false);
    }
    return esWriteDAO;
  }

  @Bean(name = "elasticSearchService")
  @Nonnull
  protected ElasticSearchService getInstance(
      final ConfigurationProvider configurationProvider,
      final QueryFilterRewriteChain queryFilterRewriteChain,
      final ElasticSearchConfiguration elasticSearchConfiguration,
      @Nullable final CustomSearchConfiguration customSearchConfiguration,
      final ESSearchDAO esSearchDAO,
      final ESWriteDAO esWriteDAO,
      @Qualifier("mappingsBuilder") final MappingsBuilder mappingsBuilder,
      @Qualifier("settingsBuilder") final SettingsBuilder settingsBuilder,
      final SearchClusterRegistry searchClusterRegistry)
      throws IOException {

    warnOpenSearch3DocIdRisk(
        elasticSearchConfiguration, searchClusterRegistry.clientFor(SearchComponent.SEARCH_V2));

    return new ElasticSearchService(
        components.getIndexBuilder(),
        configurationProvider.getSearchService(),
        configurationProvider.getElasticSearch(),
        mappingsBuilder,
        settingsBuilder,
        // Null unless V2/V3/semantic are split across clusters, in which case each index family
        // must be built by the client that owns it.
        searchClusterRegistry.entityIndexBuilderResolver(components.getIndexConvention()),
        esSearchDAO,
        new ESBrowseDAO(
            elasticSearchConfiguration,
            customSearchConfiguration,
            queryFilterRewriteChain,
            configurationProvider.getSearchService()),
        esWriteDAO);
  }

  /**
   * OpenSearch 3.x enforces the 512-byte {@code _id} limit on bulk writes (2.x did not). Schema
   * field URNs are exempt from URN length validation and, with doc-ID hashing disabled, are used
   * URL-encoded as document ids, so long schema-field URNs that indexed fine on 2.x become rejected
   * bulk items on 3.x. Checked against the Search V2 cluster, because hashing is a V2-only setting.
   * Surfaced as a startup warning rather than a failure so existing 2.x-migrated deployments can
   * start; the rejected items themselves are logged at ERROR by the bulk listener.
   */
  static void warnOpenSearch3DocIdRisk(
      @Nonnull final ElasticSearchConfiguration configuration,
      @Nullable final SearchClientShim<?> searchClient) {
    // Null in Spring tests that mock the components holder; nothing to warn about.
    if (searchClient == null
        || searchClient.getEngineType() != SearchClientShim.SearchEngineType.OPENSEARCH_3) {
      return;
    }
    final boolean hashIdEnabled =
        configuration.getEntityIndex() != null
            && configuration.getEntityIndex().getV2() != null
            && configuration.getEntityIndex().getV2().isSchemaFieldDocIdHashEnabled();
    if (!hashIdEnabled) {
      log.warn(
          "OpenSearch 3.x enforces a 512-byte _id limit on bulk writes, and schema-field document"
              + " ids use the URL-encoded URN while"
              + " elasticsearch.entityIndex.v2.docIds.schemaField.hashIdEnabled is false. Long"
              + " schema-field URNs will be rejected at index time (logged as bulk failures). Enable"
              + " ELASTICSEARCH_INDEX_DOC_IDS_SCHEMA_FIELD_HASH_ID_ENABLED for new OpenSearch 3.x"
              + " deployments.");
    }
  }
}
