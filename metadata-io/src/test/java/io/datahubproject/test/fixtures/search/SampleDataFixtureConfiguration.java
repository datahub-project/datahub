package io.datahubproject.test.fixtures.search;

import static com.linkedin.metadata.Constants.*;
import static io.datahubproject.test.search.config.SearchTestContainerConfiguration.REFRESH_INTERVAL_SECONDS;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.linkedin.entity.client.EntityClient;
import com.linkedin.entity.client.EntityClientConfig;
import com.linkedin.metadata.aspect.EntityAspect;
import com.linkedin.metadata.client.JavaEntityClient;
import com.linkedin.metadata.config.PreProcessHooks;
import com.linkedin.metadata.config.cache.EntityDocCountCacheConfiguration;
import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.config.search.SearchConfiguration;
import com.linkedin.metadata.config.search.custom.CustomSearchConfiguration;
import com.linkedin.metadata.entity.AspectDao;
import com.linkedin.metadata.entity.EntityAspectIdentifier;
import com.linkedin.metadata.entity.EntityServiceImpl;
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
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
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

  @Autowired
  @Qualifier("fixtureCustomSearchConfig")
  private CustomSearchConfiguration _customSearchConfiguration;

  @Autowired private QueryFilterRewriteChain queryFilterRewriteChain;

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
    return new IndexConventionImpl(
        IndexConventionImpl.IndexConventionConfig.builder()
            .prefix(prefix)
            .hashIdAlgo("MD5")
            .build());
  }

  @Bean(name = "longTailIndexConvention")
  protected IndexConvention longTailIndexConvention(@Qualifier("longTailPrefix") String prefix) {
    return new IndexConventionImpl(
        IndexConventionImpl.IndexConventionConfig.builder()
            .prefix(prefix)
            .hashIdAlgo("MD5")
            .build());
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
      @Qualifier("sampleDataOperationContext") OperationContext opContext) {
    return entityIndexBuildersHelper(opContext);
  }

  @Bean(name = "longTailEntityIndexBuilders")
  protected EntityIndexBuilders longTailEntityIndexBuilders(
      @Qualifier("longTailOperationContext") OperationContext opContext) {
    return entityIndexBuildersHelper(opContext);
  }

  @Bean(name = "sampleDataOperationContext")
  protected OperationContext sampleDataOperationContext(
      @Qualifier("sampleDataIndexConvention") IndexConvention indexConvention) {

    OperationContext testOpContext = TestOperationContexts.systemContextNoSearchAuthorization();

    return testOpContext.toBuilder()
        .searchContext(SearchContext.builder().indexConvention(indexConvention).build())
        .build(testOpContext.getSessionAuthentication(), true);
  }

  @Bean(name = "longTailOperationContext")
  protected OperationContext longTailOperationContext(
      @Qualifier("longTailIndexConvention") IndexConvention indexConvention) {

    OperationContext testOpContext = TestOperationContexts.systemContextNoSearchAuthorization();

    return testOpContext.toBuilder()
        .searchContext(SearchContext.builder().indexConvention(indexConvention).build())
        .build(testOpContext.getSessionAuthentication(), true);
  }

  protected EntityIndexBuilders entityIndexBuildersHelper(OperationContext opContext) {
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

  @Bean(name = "sampleDataEntitySearchService")
  protected ElasticSearchService entitySearchService(
      @Qualifier("sampleDataEntityIndexBuilders") EntityIndexBuilders indexBuilders)
      throws IOException {
    return entitySearchServiceHelper(indexBuilders);
  }

  @Bean(name = "longTailEntitySearchService")
  protected ElasticSearchService longTailEntitySearchService(
      @Qualifier("longTailEntityIndexBuilders") EntityIndexBuilders longTaiIndexBuilders)
      throws IOException {
    return entitySearchServiceHelper(longTaiIndexBuilders);
  }

  protected ElasticSearchService entitySearchServiceHelper(EntityIndexBuilders indexBuilders)
      throws IOException {

    ESSearchDAO searchDAO =
        new ESSearchDAO(
            _searchClient,
            false,
            ELASTICSEARCH_IMPLEMENTATION_ELASTICSEARCH,
            _searchConfiguration,
            _customSearchConfiguration,
            queryFilterRewriteChain,
            true);
    ESBrowseDAO browseDAO =
        new ESBrowseDAO(
            _searchClient,
            _searchConfiguration,
            _customSearchConfiguration,
            queryFilterRewriteChain);
    ESWriteDAO writeDAO = new ESWriteDAO(_searchClient, _bulkProcessor, 1);
    return new ElasticSearchService(indexBuilders, searchDAO, browseDAO, writeDAO);
  }

  @Bean(name = "sampleDataSearchService")
  @Nonnull
  protected SearchService searchService(
      @Qualifier("sampleDataOperationContext") OperationContext sampleDataOperationContext,
      @Qualifier("sampleDataEntitySearchService") ElasticSearchService entitySearchService,
      @Qualifier("sampleDataEntityIndexBuilders") EntityIndexBuilders indexBuilders,
      @Qualifier("sampleDataPrefix") String prefix,
      @Qualifier("sampleDataFixtureName") String sampleDataFixtureName)
      throws IOException {
    return searchServiceHelper(
        sampleDataOperationContext,
        entitySearchService,
        indexBuilders,
        prefix,
        sampleDataFixtureName);
  }

  @Bean(name = "longTailSearchService")
  @Nonnull
  protected SearchService longTailSearchService(
      @Qualifier("longTailOperationContext") OperationContext longtailOperationContext,
      @Qualifier("longTailEntitySearchService") ElasticSearchService longTailEntitySearchService,
      @Qualifier("longTailEntityIndexBuilders") EntityIndexBuilders longTailIndexBuilders,
      @Qualifier("longTailPrefix") String longTailPrefix,
      @Qualifier("longTailFixtureName") String longTailFixtureName)
      throws IOException {
    return searchServiceHelper(
        longtailOperationContext,
        longTailEntitySearchService,
        longTailIndexBuilders,
        longTailPrefix,
        longTailFixtureName);
  }

  public SearchService searchServiceHelper(
      OperationContext opContext,
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
                opContext.getEntityRegistry(),
                entitySearchService,
                entityDocCountCacheConfiguration),
            new CachingEntitySearchService(cacheManager, entitySearchService, batchSize, false),
            ranker);

    // Build indices & write fixture data
    indexBuilders.reindexAll(Collections.emptySet());

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
      @Qualifier("sampleDataEntitySearchService") ElasticSearchService entitySearchService) {
    return entityClientHelper(searchService, entitySearchService);
  }

  @Bean(name = "longTailEntityClient")
  @Nonnull
  protected EntityClient longTailEntityClient(
      @Qualifier("sampleDataSearchService") SearchService searchService,
      @Qualifier("sampleDataEntitySearchService") ElasticSearchService entitySearchService) {
    return entityClientHelper(searchService, entitySearchService);
  }

  private EntityClient entityClientHelper(
      SearchService searchService, ElasticSearchService entitySearchService) {
    CachingEntitySearchService cachingEntitySearchService =
        new CachingEntitySearchService(
            new ConcurrentMapCacheManager(), entitySearchService, 1, false);

    AspectDao mockAspectDao = mock(AspectDao.class);
    when(mockAspectDao.batchGet(anySet(), anyBoolean()))
        .thenAnswer(
            args -> {
              Set<EntityAspectIdentifier> ids = args.getArgument(0);
              return ids.stream()
                  .map(
                      id -> {
                        EntityAspect mockEntityAspect = mock(EntityAspect.class);
                        when(mockEntityAspect.getUrn()).thenReturn(id.getUrn());
                        when(mockEntityAspect.getAspect()).thenReturn(id.getAspect());
                        when(mockEntityAspect.getVersion()).thenReturn(id.getVersion());
                        return Map.entry(id, mockEntityAspect);
                      })
                  .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
            });

    PreProcessHooks preProcessHooks = new PreProcessHooks();
    preProcessHooks.setUiEnabled(true);
    return new JavaEntityClient(
        new EntityServiceImpl(mockAspectDao, null, true, preProcessHooks, true),
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
