package io.datahubproject.test.fixtures.search;

import static com.linkedin.metadata.Constants.*;
import static io.datahubproject.test.search.SearchTestUtils.getGraphQueryConfiguration;

import com.linkedin.entity.client.EntityClient;
import com.linkedin.entity.client.EntityClientConfig;
import com.linkedin.metadata.client.JavaEntityClient;
import com.linkedin.metadata.config.DataHubAppConfiguration;
import com.linkedin.metadata.config.MetadataChangeProposalConfig;
import com.linkedin.metadata.config.PreProcessHooks;
import com.linkedin.metadata.config.cache.CacheConfiguration;
import com.linkedin.metadata.config.cache.EntityDocCountCacheConfiguration;
import com.linkedin.metadata.config.cache.SearchCacheConfiguration;
import com.linkedin.metadata.config.cache.SearchLineageCacheConfiguration;
import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.config.search.SearchConfiguration;
import com.linkedin.metadata.config.search.custom.CustomSearchConfiguration;
import com.linkedin.metadata.entity.EntityServiceImpl;
import com.linkedin.metadata.graph.elastic.ESGraphQueryDAO;
import com.linkedin.metadata.graph.elastic.ESGraphWriteDAO;
import com.linkedin.metadata.graph.elastic.ElasticSearchGraphService;
import com.linkedin.metadata.models.registry.LineageRegistry;
import com.linkedin.metadata.search.LineageSearchService;
import com.linkedin.metadata.search.SearchService;
import com.linkedin.metadata.search.cache.EntityDocCountCache;
import com.linkedin.metadata.search.client.CachingEntitySearchService;
import com.linkedin.metadata.search.elasticsearch.ElasticSearchService;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ESIndexBuilder;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.EntityIndexBuilders;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.SettingsBuilder;
import com.linkedin.metadata.search.elasticsearch.query.ESBrowseDAO;
import com.linkedin.metadata.search.elasticsearch.query.ESSearchDAO;
import com.linkedin.metadata.search.elasticsearch.query.filter.QueryFilterRewriteChain;
import com.linkedin.metadata.search.elasticsearch.update.ESBulkProcessor;
import com.linkedin.metadata.search.elasticsearch.update.ESWriteDAO;
import com.linkedin.metadata.search.ranker.SearchRanker;
import com.linkedin.metadata.search.ranker.SimpleRanker;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.metadata.utils.elasticsearch.IndexConventionImpl;
import com.linkedin.metadata.version.GitVersion;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.SearchContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import io.datahubproject.test.search.config.SearchCommonTestConfiguration;
import io.datahubproject.test.search.config.SearchTestContainerConfiguration;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nonnull;
import org.opensearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.cache.CacheManager;
import org.springframework.cache.concurrent.ConcurrentMapCacheManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;

@TestConfiguration
@Import(SearchCommonTestConfiguration.class)
public class SearchLineageFixtureConfiguration {

  @Autowired private ESBulkProcessor bulkProcessor;

  @Autowired private RestHighLevelClient searchClient;

  @Autowired private SearchConfiguration searchConfiguration;

  @Autowired
  @Qualifier("fixtureCustomSearchConfig")
  private CustomSearchConfiguration customSearchConfiguration;

  @Bean(name = "searchLineagePrefix")
  protected String indexPrefix() {
    return "srchlin";
  }

  @Bean(name = "searchLineageIndexConvention")
  protected IndexConvention indexConvention(@Qualifier("searchLineagePrefix") String prefix) {
    return new IndexConventionImpl(
        IndexConventionImpl.IndexConventionConfig.builder()
            .prefix(prefix)
            .hashIdAlgo("MD5")
            .build());
  }

  @Bean(name = "searchLineageFixtureName")
  protected String fixtureName() {
    return "search_lineage";
  }

  @Bean(name = "lineageAppConfig")
  protected DataHubAppConfiguration searchLineageAppConfiguration() {
    DataHubAppConfiguration conf = new DataHubAppConfiguration();
    conf.setCache(new CacheConfiguration());
    conf.getCache().setSearch(new SearchCacheConfiguration());
    conf.getCache().getSearch().setLineage(new SearchLineageCacheConfiguration());
    conf.getCache().getSearch().getLineage().setLightningThreshold(300);
    conf.getCache().getSearch().getLineage().setTtlSeconds(30);
    conf.setMetadataChangeProposal(new MetadataChangeProposalConfig());
    conf.getMetadataChangeProposal()
        .setSideEffects(new MetadataChangeProposalConfig.SideEffectsConfig());
    conf.getMetadataChangeProposal()
        .getSideEffects()
        .setSchemaField(new MetadataChangeProposalConfig.SideEffectConfig());
    conf.getMetadataChangeProposal().getSideEffects().getSchemaField().setEnabled(false);
    return conf;
  }

  @Bean(name = "searchLineageEntityIndexBuilders")
  protected EntityIndexBuilders entityIndexBuilders(
      @Qualifier("searchLineageOperationContext") OperationContext opContext) {
    GitVersion gitVersion = new GitVersion("0.0.0-test", "123456", Optional.empty());
    ESIndexBuilder indexBuilder =
        new ESIndexBuilder(
            searchClient,
            1,
            0,
            1,
            1,
            Map.of(),
            true,
            false,
            false,
            new ElasticSearchConfiguration(),
            gitVersion);
    SettingsBuilder settingsBuilder = new SettingsBuilder(null);
    return new EntityIndexBuilders(
        indexBuilder,
        opContext.getEntityRegistry(),
        opContext.getSearchContext().getIndexConvention(),
        settingsBuilder);
  }

  @Bean(name = "searchLineageEntitySearchService")
  protected ElasticSearchService entitySearchService(
      @Qualifier("searchLineageEntityIndexBuilders") EntityIndexBuilders indexBuilders,
      final QueryFilterRewriteChain queryFilterRewriteChain) {
    ESSearchDAO searchDAO =
        new ESSearchDAO(
            searchClient,
            false,
            ELASTICSEARCH_IMPLEMENTATION_ELASTICSEARCH,
            searchConfiguration,
            customSearchConfiguration,
            queryFilterRewriteChain);
    ESBrowseDAO browseDAO =
        new ESBrowseDAO(
            searchClient, searchConfiguration, customSearchConfiguration, queryFilterRewriteChain);
    ESWriteDAO writeDAO = new ESWriteDAO(searchClient, bulkProcessor, 1);

    return new ElasticSearchService(indexBuilders, searchDAO, browseDAO, writeDAO);
  }

  @Bean(name = "searchLineageOperationContext")
  protected OperationContext searchLineageOperationContext(
      @Qualifier("searchLineageIndexConvention") IndexConvention indexConvention) {

    OperationContext testOpContext = TestOperationContexts.systemContextNoSearchAuthorization();

    return testOpContext.toBuilder()
        .searchContext(SearchContext.builder().indexConvention(indexConvention).build())
        .build(testOpContext.getSessionAuthentication(), true);
  }

  @Bean(name = "searchLineageESIndexBuilder")
  @Nonnull
  protected ESIndexBuilder esIndexBuilder() {
    GitVersion gitVersion = new GitVersion("0.0.0-test", "123456", Optional.empty());
    return new ESIndexBuilder(
        searchClient,
        1,
        1,
        1,
        1,
        Map.of(),
        true,
        true,
        false,
        new ElasticSearchConfiguration(),
        gitVersion);
  }

  @Bean(name = "searchLineageGraphService")
  @Nonnull
  protected ElasticSearchGraphService graphService(
      @Qualifier("searchLineageOperationContext") OperationContext opContext,
      @Qualifier("searchLineageESIndexBuilder") ESIndexBuilder indexBuilder) {
    LineageRegistry lineageRegistry = new LineageRegistry(opContext.getEntityRegistry());
    IndexConvention indexConvention = opContext.getSearchContext().getIndexConvention();
    ElasticSearchGraphService graphService =
        new ElasticSearchGraphService(
            lineageRegistry,
            bulkProcessor,
            indexConvention,
            new ESGraphWriteDAO(indexConvention, bulkProcessor, 1, getGraphQueryConfiguration()),
            new ESGraphQueryDAO(
                searchClient, lineageRegistry, indexConvention, getGraphQueryConfiguration()),
            indexBuilder,
            indexConvention.getIdHashAlgo());
    graphService.reindexAll(Collections.emptySet());
    return graphService;
  }

  @Bean(name = "searchLineageLineageSearchService")
  @Nonnull
  protected LineageSearchService lineageSearchService(
      @Qualifier("searchLineageSearchService") SearchService searchService,
      @Qualifier("searchLineageGraphService") ElasticSearchGraphService graphService,
      @Qualifier("searchLineagePrefix") String prefix,
      @Qualifier("searchLineageFixtureName") String fixtureName,
      @Qualifier("lineageAppConfig") DataHubAppConfiguration appConfig)
      throws IOException {

    // Load fixture data (after graphService mappings applied)
    FixtureReader.builder()
        .bulkProcessor(bulkProcessor)
        .fixtureName(fixtureName)
        .targetIndexPrefix(prefix)
        .refreshIntervalSeconds(SearchTestContainerConfiguration.REFRESH_INTERVAL_SECONDS)
        .build()
        .read();

    return new LineageSearchService(searchService, graphService, null, false, appConfig);
  }

  @Bean(name = "searchLineageSearchService")
  @Nonnull
  protected SearchService searchService(
      @Qualifier("searchLineageOperationContext") OperationContext opContext,
      @Qualifier("searchLineageEntitySearchService") ElasticSearchService entitySearchService,
      @Qualifier("searchLineageEntityIndexBuilders") EntityIndexBuilders indexBuilders)
      throws IOException {

    int batchSize = 100;
    SearchRanker<Double> ranker = new SimpleRanker();
    CacheManager cacheManager = new ConcurrentMapCacheManager();
    EntityDocCountCacheConfiguration entityDocCountCacheConfiguration =
        new EntityDocCountCacheConfiguration();
    entityDocCountCacheConfiguration.setTtlSeconds(600L);

    SearchService service =
        new SearchService(
            new EntityDocCountCache(
                opContext.getEntityRegistry(),
                entitySearchService,
                entityDocCountCacheConfiguration),
            new CachingEntitySearchService(cacheManager, entitySearchService, batchSize, false),
            ranker);

    // Build indices
    indexBuilders.reindexAll(Collections.emptySet());

    return service;
  }

  @Bean(name = "searchLineageEntityClient")
  @Nonnull
  protected EntityClient entityClient(
      @Qualifier("searchLineageSearchService") SearchService searchService,
      @Qualifier("searchLineageEntitySearchService") ElasticSearchService entitySearchService) {
    CachingEntitySearchService cachingEntitySearchService =
        new CachingEntitySearchService(
            new ConcurrentMapCacheManager(), entitySearchService, 1, false);

    PreProcessHooks preProcessHooks = new PreProcessHooks();
    preProcessHooks.setUiEnabled(true);
    return new JavaEntityClient(
        new EntityServiceImpl(null, null, true, preProcessHooks, true),
        null,
        entitySearchService,
        cachingEntitySearchService,
        searchService,
        null,
        null,
        null,
        null,
        EntityClientConfig.builder().batchGetV2Size(1).build());
  }
}
