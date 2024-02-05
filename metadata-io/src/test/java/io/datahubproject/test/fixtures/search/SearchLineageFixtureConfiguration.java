package io.datahubproject.test.fixtures.search;

import static com.linkedin.metadata.Constants.*;

import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.client.JavaEntityClient;
import com.linkedin.metadata.config.PreProcessHooks;
import com.linkedin.metadata.config.cache.EntityDocCountCacheConfiguration;
import com.linkedin.metadata.config.cache.SearchLineageCacheConfiguration;
import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.config.search.GraphQueryConfiguration;
import com.linkedin.metadata.config.search.SearchConfiguration;
import com.linkedin.metadata.config.search.custom.CustomSearchConfiguration;
import com.linkedin.metadata.entity.EntityServiceImpl;
import com.linkedin.metadata.graph.elastic.ESGraphQueryDAO;
import com.linkedin.metadata.graph.elastic.ESGraphWriteDAO;
import com.linkedin.metadata.graph.elastic.ElasticSearchGraphService;
import com.linkedin.metadata.models.registry.EntityRegistry;
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
import com.linkedin.metadata.search.elasticsearch.update.ESBulkProcessor;
import com.linkedin.metadata.search.elasticsearch.update.ESWriteDAO;
import com.linkedin.metadata.search.ranker.SearchRanker;
import com.linkedin.metadata.search.ranker.SimpleRanker;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.metadata.utils.elasticsearch.IndexConventionImpl;
import com.linkedin.metadata.version.GitVersion;
import io.datahubproject.test.search.config.SearchCommonTestConfiguration;
import io.datahubproject.test.search.config.SearchTestContainerConfiguration;
import java.io.IOException;
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

  @Autowired private ESBulkProcessor _bulkProcessor;

  @Autowired private RestHighLevelClient _searchClient;

  @Autowired private SearchConfiguration _searchConfiguration;

  @Autowired private CustomSearchConfiguration _customSearchConfiguration;

  @Bean(name = "searchLineagePrefix")
  protected String indexPrefix() {
    return "srchlin";
  }

  @Bean(name = "searchLineageIndexConvention")
  protected IndexConvention indexConvention(@Qualifier("searchLineagePrefix") String prefix) {
    return new IndexConventionImpl(prefix);
  }

  @Bean(name = "searchLineageFixtureName")
  protected String fixtureName() {
    return "search_lineage";
  }

  @Bean(name = "lineageCacheConfiguration")
  protected SearchLineageCacheConfiguration searchLineageCacheConfiguration() {
    SearchLineageCacheConfiguration conf = new SearchLineageCacheConfiguration();
    conf.setLightningThreshold(300);
    conf.setTtlSeconds(30);
    return conf;
  }

  @Bean(name = "searchLineageEntityIndexBuilders")
  protected EntityIndexBuilders entityIndexBuilders(
      @Qualifier("entityRegistry") EntityRegistry entityRegistry,
      @Qualifier("searchLineageIndexConvention") IndexConvention indexConvention) {
    GitVersion gitVersion = new GitVersion("0.0.0-test", "123456", Optional.empty());
    ESIndexBuilder indexBuilder =
        new ESIndexBuilder(
            _searchClient,
            1,
            0,
            1,
            1,
            Map.of(),
            true,
            false,
            new ElasticSearchConfiguration(),
            gitVersion);
    SettingsBuilder settingsBuilder = new SettingsBuilder(null);
    return new EntityIndexBuilders(indexBuilder, entityRegistry, indexConvention, settingsBuilder);
  }

  @Bean(name = "searchLineageEntitySearchService")
  protected ElasticSearchService entitySearchService(
      @Qualifier("entityRegistry") EntityRegistry entityRegistry,
      @Qualifier("searchLineageEntityIndexBuilders") EntityIndexBuilders indexBuilders,
      @Qualifier("searchLineageIndexConvention") IndexConvention indexConvention) {
    ESSearchDAO searchDAO =
        new ESSearchDAO(
            entityRegistry,
            _searchClient,
            indexConvention,
            false,
            ELASTICSEARCH_IMPLEMENTATION_ELASTICSEARCH,
            _searchConfiguration,
            null);
    ESBrowseDAO browseDAO =
        new ESBrowseDAO(
            entityRegistry,
            _searchClient,
            indexConvention,
            _searchConfiguration,
            _customSearchConfiguration);
    ESWriteDAO writeDAO =
        new ESWriteDAO(entityRegistry, _searchClient, indexConvention, _bulkProcessor, 1);
    return new ElasticSearchService(indexBuilders, searchDAO, browseDAO, writeDAO);
  }

  @Bean(name = "searchLineageESIndexBuilder")
  @Nonnull
  protected ESIndexBuilder esIndexBuilder() {
    GitVersion gitVersion = new GitVersion("0.0.0-test", "123456", Optional.empty());
    return new ESIndexBuilder(
        _searchClient,
        1,
        1,
        1,
        1,
        Map.of(),
        true,
        true,
        new ElasticSearchConfiguration(),
        gitVersion);
  }

  @Bean(name = "searchLineageGraphService")
  @Nonnull
  protected ElasticSearchGraphService graphService(
      @Qualifier("entityRegistry") EntityRegistry entityRegistry,
      @Qualifier("searchLineageESIndexBuilder") ESIndexBuilder indexBuilder,
      @Qualifier("searchLineageIndexConvention") IndexConvention indexConvention) {
    LineageRegistry lineageRegistry = new LineageRegistry(entityRegistry);
    ElasticSearchGraphService graphService =
        new ElasticSearchGraphService(
            lineageRegistry,
            _bulkProcessor,
            indexConvention,
            new ESGraphWriteDAO(indexConvention, _bulkProcessor, 1),
            new ESGraphQueryDAO(
                _searchClient,
                lineageRegistry,
                indexConvention,
                GraphQueryConfiguration.testDefaults),
            indexBuilder);
    graphService.configure();
    return graphService;
  }

  @Bean(name = "searchLineageLineageSearchService")
  @Nonnull
  protected LineageSearchService lineageSearchService(
      @Qualifier("searchLineageSearchService") SearchService searchService,
      @Qualifier("searchLineageGraphService") ElasticSearchGraphService graphService,
      @Qualifier("searchLineagePrefix") String prefix,
      @Qualifier("searchLineageFixtureName") String fixtureName,
      @Qualifier("lineageCacheConfiguration") SearchLineageCacheConfiguration cacheConfiguration)
      throws IOException {

    // Load fixture data (after graphService mappings applied)
    FixtureReader.builder()
        .bulkProcessor(_bulkProcessor)
        .fixtureName(fixtureName)
        .targetIndexPrefix(prefix)
        .refreshIntervalSeconds(SearchTestContainerConfiguration.REFRESH_INTERVAL_SECONDS)
        .build()
        .read();

    return new LineageSearchService(searchService, graphService, null, false, cacheConfiguration);
  }

  @Bean(name = "searchLineageSearchService")
  @Nonnull
  protected SearchService searchService(
      @Qualifier("entityRegistry") EntityRegistry entityRegistry,
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
                entityRegistry, entitySearchService, entityDocCountCacheConfiguration),
            new CachingEntitySearchService(cacheManager, entitySearchService, batchSize, false),
            ranker);

    // Build indices
    indexBuilders.reindexAll();

    return service;
  }

  @Bean(name = "searchLineageEntityClient")
  @Nonnull
  protected EntityClient entityClient(
      @Qualifier("searchLineageSearchService") SearchService searchService,
      @Qualifier("searchLineageEntitySearchService") ElasticSearchService entitySearchService,
      @Qualifier("entityRegistry") EntityRegistry entityRegistry) {
    CachingEntitySearchService cachingEntitySearchService =
        new CachingEntitySearchService(
            new ConcurrentMapCacheManager(), entitySearchService, 1, false);

    PreProcessHooks preProcessHooks = new PreProcessHooks();
    preProcessHooks.setUiEnabled(true);
    return new JavaEntityClient(
        new EntityServiceImpl(null, null, entityRegistry, true, null, preProcessHooks),
        null,
        entitySearchService,
        cachingEntitySearchService,
        searchService,
        null,
        null,
        null,
        null);
  }
}
