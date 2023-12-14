package io.datahubproject.test.fixtures.search;

import static com.linkedin.metadata.Constants.*;
import static io.datahubproject.test.search.config.SearchTestContainerConfiguration.REFRESH_INTERVAL_SECONDS;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.client.JavaEntityClient;
import com.linkedin.metadata.config.PreProcessHooks;
import com.linkedin.metadata.config.cache.EntityDocCountCacheConfiguration;
import com.linkedin.metadata.config.search.CustomConfiguration;
import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.config.search.SearchConfiguration;
import com.linkedin.metadata.config.search.custom.CustomSearchConfiguration;
import com.linkedin.metadata.entity.AspectDao;
import com.linkedin.metadata.entity.EntityAspect;
import com.linkedin.metadata.entity.EntityAspectIdentifier;
import com.linkedin.metadata.entity.EntityServiceImpl;
import com.linkedin.metadata.models.registry.EntityRegistry;
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
public class SampleDataFixtureConfiguration {
  /**
   * Interested in adding more fixtures? Here's what you will need to update? 1. Create a new
   * indexPrefix and FixtureName. Both are needed or else all fixtures will load on top of each
   * other, overwriting each other 2. Create a new IndexConvention, IndexBuilder, and EntityClient.
   * These are needed to index a different set of entities.
   */
  @Autowired private ESBulkProcessor _bulkProcessor;

  @Autowired private RestHighLevelClient _searchClient;

  @Autowired private RestHighLevelClient _longTailSearchClient;

  @Autowired private SearchConfiguration _searchConfiguration;

  @Autowired private CustomSearchConfiguration _customSearchConfiguration;

  @Bean(name = "sampleDataPrefix")
  protected String sampleDataPrefix() {
    return "smpldat";
  }

  @Bean(name = "longTailPrefix")
  protected String longTailIndexPrefix() {
    return "lngtl";
  }

  @Bean(name = "sampleDataIndexConvention")
  protected IndexConvention indexConvention(@Qualifier("sampleDataPrefix") String prefix) {
    return new IndexConventionImpl(prefix);
  }

  @Bean(name = "longTailIndexConvention")
  protected IndexConvention longTailIndexConvention(@Qualifier("longTailPrefix") String prefix) {
    return new IndexConventionImpl(prefix);
  }

  @Bean(name = "sampleDataFixtureName")
  protected String sampleDataFixtureName() {
    return "sample_data";
  }

  @Bean(name = "longTailFixtureName")
  protected String longTailFixtureName() {
    return "long_tail";
  }

  @Bean(name = "sampleDataEntityIndexBuilders")
  protected EntityIndexBuilders entityIndexBuilders(
      @Qualifier("entityRegistry") EntityRegistry entityRegistry,
      @Qualifier("sampleDataIndexConvention") IndexConvention indexConvention) {
    return entityIndexBuildersHelper(entityRegistry, indexConvention);
  }

  @Bean(name = "longTailEntityIndexBuilders")
  protected EntityIndexBuilders longTailEntityIndexBuilders(
      @Qualifier("entityRegistry") EntityRegistry longTailEntityRegistry,
      @Qualifier("longTailIndexConvention") IndexConvention indexConvention) {
    return entityIndexBuildersHelper(longTailEntityRegistry, indexConvention);
  }

  protected EntityIndexBuilders entityIndexBuildersHelper(
      EntityRegistry entityRegistry, IndexConvention indexConvention) {
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

  @Bean(name = "sampleDataEntitySearchService")
  protected ElasticSearchService entitySearchService(
      @Qualifier("entityRegistry") EntityRegistry entityRegistry,
      @Qualifier("sampleDataEntityIndexBuilders") EntityIndexBuilders indexBuilders,
      @Qualifier("sampleDataIndexConvention") IndexConvention indexConvention)
      throws IOException {
    return entitySearchServiceHelper(entityRegistry, indexBuilders, indexConvention);
  }

  @Bean(name = "longTailEntitySearchService")
  protected ElasticSearchService longTailEntitySearchService(
      @Qualifier("entityRegistry") EntityRegistry longTailEntityRegistry,
      @Qualifier("longTailEntityIndexBuilders") EntityIndexBuilders longTailEndexBuilders,
      @Qualifier("longTailIndexConvention") IndexConvention longTailIndexConvention)
      throws IOException {
    return entitySearchServiceHelper(
        longTailEntityRegistry, longTailEndexBuilders, longTailIndexConvention);
  }

  protected ElasticSearchService entitySearchServiceHelper(
      EntityRegistry entityRegistry,
      EntityIndexBuilders indexBuilders,
      IndexConvention indexConvention)
      throws IOException {
    CustomConfiguration customConfiguration = new CustomConfiguration();
    customConfiguration.setEnabled(true);
    customConfiguration.setFile("search_config_fixture_test.yml");
    CustomSearchConfiguration customSearchConfiguration =
        customConfiguration.resolve(new YAMLMapper());

    ESSearchDAO searchDAO =
        new ESSearchDAO(
            entityRegistry,
            _searchClient,
            indexConvention,
            false,
            ELASTICSEARCH_IMPLEMENTATION_ELASTICSEARCH,
            _searchConfiguration,
            customSearchConfiguration);
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

  @Bean(name = "sampleDataSearchService")
  @Nonnull
  protected SearchService searchService(
      @Qualifier("entityRegistry") EntityRegistry entityRegistry,
      @Qualifier("sampleDataEntitySearchService") ElasticSearchService entitySearchService,
      @Qualifier("sampleDataEntityIndexBuilders") EntityIndexBuilders indexBuilders,
      @Qualifier("sampleDataPrefix") String prefix,
      @Qualifier("sampleDataFixtureName") String sampleDataFixtureName)
      throws IOException {
    return searchServiceHelper(
        entityRegistry, entitySearchService, indexBuilders, prefix, sampleDataFixtureName);
  }

  @Bean(name = "longTailSearchService")
  @Nonnull
  protected SearchService longTailSearchService(
      @Qualifier("entityRegistry") EntityRegistry longTailEntityRegistry,
      @Qualifier("longTailEntitySearchService") ElasticSearchService longTailEntitySearchService,
      @Qualifier("longTailEntityIndexBuilders") EntityIndexBuilders longTailIndexBuilders,
      @Qualifier("longTailPrefix") String longTailPrefix,
      @Qualifier("longTailFixtureName") String longTailFixtureName)
      throws IOException {
    return searchServiceHelper(
        longTailEntityRegistry,
        longTailEntitySearchService,
        longTailIndexBuilders,
        longTailPrefix,
        longTailFixtureName);
  }

  public SearchService searchServiceHelper(
      EntityRegistry entityRegistry,
      ElasticSearchService entitySearchService,
      EntityIndexBuilders indexBuilders,
      String prefix,
      String fixtureName)
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

    // Build indices & write fixture data
    indexBuilders.reindexAll();

    FixtureReader.builder()
        .bulkProcessor(_bulkProcessor)
        .fixtureName(fixtureName)
        .targetIndexPrefix(prefix)
        .refreshIntervalSeconds(REFRESH_INTERVAL_SECONDS)
        .build()
        .read();

    return service;
  }

  @Bean(name = "sampleDataEntityClient")
  @Nonnull
  protected EntityClient entityClient(
      @Qualifier("sampleDataSearchService") SearchService searchService,
      @Qualifier("sampleDataEntitySearchService") ElasticSearchService entitySearchService,
      @Qualifier("entityRegistry") EntityRegistry entityRegistry) {
    return entityClientHelper(searchService, entitySearchService, entityRegistry);
  }

  @Bean(name = "longTailEntityClient")
  @Nonnull
  protected EntityClient longTailEntityClient(
      @Qualifier("sampleDataSearchService") SearchService searchService,
      @Qualifier("sampleDataEntitySearchService") ElasticSearchService entitySearchService,
      @Qualifier("entityRegistry") EntityRegistry longTailEntityRegistry) {
    return entityClientHelper(searchService, entitySearchService, longTailEntityRegistry);
  }

  private EntityClient entityClientHelper(
      SearchService searchService,
      ElasticSearchService entitySearchService,
      EntityRegistry entityRegistry) {
    CachingEntitySearchService cachingEntitySearchService =
        new CachingEntitySearchService(
            new ConcurrentMapCacheManager(), entitySearchService, 1, false);

    AspectDao mockAspectDao = mock(AspectDao.class);
    when(mockAspectDao.batchGet(anySet()))
        .thenReturn(Map.of(mock(EntityAspectIdentifier.class), mock(EntityAspect.class)));

    PreProcessHooks preProcessHooks = new PreProcessHooks();
    preProcessHooks.setUiEnabled(true);
    return new JavaEntityClient(
        new EntityServiceImpl(mockAspectDao, null, entityRegistry, true, null, preProcessHooks),
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
