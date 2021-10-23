package com.linkedin.metadata.search;

import com.datahub.test.Snapshot;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.TestEntityUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.models.registry.SnapshotEntityRegistry;
import com.linkedin.metadata.search.elasticsearch.ElasticSearchService;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ESIndexBuilders;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.SettingsBuilder;
import com.linkedin.metadata.search.elasticsearch.query.ESBrowseDAO;
import com.linkedin.metadata.search.elasticsearch.query.ESSearchDAO;
import com.linkedin.metadata.search.elasticsearch.update.ESWriteDAO;
import com.linkedin.metadata.search.ranker.SimpleRanker;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.metadata.utils.elasticsearch.IndexConventionImpl;
import java.util.Collections;
import javax.annotation.Nonnull;
import org.apache.http.HttpHost;
import org.apache.http.impl.nio.reactor.IOReactorConfig;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.cache.CacheManager;
import org.springframework.cache.concurrent.ConcurrentMapCacheManager;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import static com.linkedin.metadata.ElasticSearchTestUtils.syncAfterWrite;
import static org.testng.Assert.assertEquals;


public class SearchServiceTest {

  private ElasticsearchContainer _elasticsearchContainer;
  private RestHighLevelClient _searchClient;
  private EntityRegistry _entityRegistry;
  private IndexConvention _indexConvention;
  private SettingsBuilder _settingsBuilder;
  private ElasticSearchService _elasticSearchService;
  private CacheManager _cacheManager;
  private SearchService _searchService;

  private static final String IMAGE_NAME = "docker.elastic.co/elasticsearch/elasticsearch:7.9.3";
  private static final int HTTP_PORT = 9200;
  private static final String ENTITY_NAME = "testEntity";

  @BeforeTest
  public void setup() {
    _entityRegistry = new SnapshotEntityRegistry(new Snapshot());
    _indexConvention = new IndexConventionImpl(null);
    _elasticsearchContainer = new ElasticsearchContainer(IMAGE_NAME);
    _settingsBuilder = new SettingsBuilder(Collections.emptyList());
    _elasticsearchContainer.start();
    _searchClient = buildRestClient();
    _elasticSearchService = buildEntitySearchService();
    _elasticSearchService.configure();
    _cacheManager = new ConcurrentMapCacheManager();
    _searchService = new SearchService(_entityRegistry, _elasticSearchService, new SimpleRanker(), _cacheManager, 100);
  }

  @BeforeMethod
  public void wipe() throws Exception {
    _elasticSearchService.clear();
    syncAfterWrite(_searchClient);
  }

  @Nonnull
  private RestHighLevelClient buildRestClient() {
    final RestClientBuilder builder =
        RestClient.builder(new HttpHost("localhost", _elasticsearchContainer.getMappedPort(HTTP_PORT), "http"))
            .setHttpClientConfigCallback(httpAsyncClientBuilder -> httpAsyncClientBuilder.setDefaultIOReactorConfig(
                IOReactorConfig.custom().setIoThreadCount(1).build()));

    builder.setRequestConfigCallback(requestConfigBuilder -> requestConfigBuilder.
        setConnectionRequestTimeout(3000));

    return new RestHighLevelClient(builder);
  }

  @Nonnull
  private ElasticSearchService buildEntitySearchService() {
    ESIndexBuilders indexBuilders =
        new ESIndexBuilders(_entityRegistry, _searchClient, _indexConvention, _settingsBuilder);
    ESSearchDAO searchDAO = new ESSearchDAO(_entityRegistry, _searchClient, _indexConvention);
    ESBrowseDAO browseDAO = new ESBrowseDAO(_entityRegistry, _searchClient, _indexConvention);
    ESWriteDAO writeDAO = new ESWriteDAO(_entityRegistry, _searchClient, _indexConvention, 1, 1, 1, 1);
    return new ElasticSearchService(indexBuilders, searchDAO, browseDAO, writeDAO);
  }

  private void clearCache() {
    _cacheManager.getCacheNames().forEach(cache -> _cacheManager.getCache(cache).clear());
  }

  @AfterTest
  public void tearDown() {
    _elasticsearchContainer.stop();
  }

  @Test
  public void testSearchService() throws Exception {
    SearchResult searchResult =
        _searchService.searchAcrossEntities(ImmutableList.of(ENTITY_NAME), "test", null, null, 0, 10);
    assertEquals(searchResult.getNumEntities().intValue(), 0);
    searchResult = _searchService.searchAcrossEntities(ImmutableList.of(), "test", null, null, 0, 10);
    assertEquals(searchResult.getNumEntities().intValue(), 0);
    clearCache();

    Urn urn = new TestEntityUrn("test", "testUrn", "VALUE_1");
    ObjectNode document = JsonNodeFactory.instance.objectNode();
    document.set("urn", JsonNodeFactory.instance.textNode(urn.toString()));
    document.set("keyPart1", JsonNodeFactory.instance.textNode("test"));
    document.set("textFieldOverride", JsonNodeFactory.instance.textNode("textFieldOverride"));
    document.set("browsePaths", JsonNodeFactory.instance.textNode("/a/b/c"));
    _elasticSearchService.upsertDocument(ENTITY_NAME, document.toString(), urn.toString());
    syncAfterWrite(_searchClient);

    searchResult = _searchService.searchAcrossEntities(ImmutableList.of(), "test", null, null, 0, 10);
    assertEquals(searchResult.getNumEntities().intValue(), 1);
    assertEquals(searchResult.getEntities().get(0).getEntity(), urn);
    clearCache();

    Urn urn2 = new TestEntityUrn("test", "testUrn2", "VALUE_2");
    ObjectNode document2 = JsonNodeFactory.instance.objectNode();
    document2.set("urn", JsonNodeFactory.instance.textNode(urn2.toString()));
    document2.set("keyPart1", JsonNodeFactory.instance.textNode("random"));
    document2.set("textFieldOverride", JsonNodeFactory.instance.textNode("textFieldOverride2"));
    document2.set("browsePaths", JsonNodeFactory.instance.textNode("/b/c"));
    _elasticSearchService.upsertDocument(ENTITY_NAME, document2.toString(), urn2.toString());
    syncAfterWrite(_searchClient);

    searchResult = _searchService.searchAcrossEntities(ImmutableList.of(), "test", null, null, 0, 10);
    assertEquals(searchResult.getNumEntities().intValue(), 1);
    assertEquals(searchResult.getEntities().get(0).getEntity(), urn);
    clearCache();

    _elasticSearchService.deleteDocument(ENTITY_NAME, urn.toString());
    _elasticSearchService.deleteDocument(ENTITY_NAME, urn2.toString());
    syncAfterWrite(_searchClient);
    searchResult = _searchService.searchAcrossEntities(ImmutableList.of(), "test", null, null, 0, 10);
    assertEquals(searchResult.getNumEntities().intValue(), 0);
  }
}
