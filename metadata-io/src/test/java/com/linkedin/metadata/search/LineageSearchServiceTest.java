package com.linkedin.metadata.search;

import com.datahub.test.Snapshot;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.TestEntityUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.schema.annotation.PathSpecBasedSchemaAnnotationVisitor;
import com.linkedin.metadata.ESTestConfiguration;
import com.linkedin.metadata.TestEntityUtil;
import com.linkedin.metadata.config.EntityDocCountCacheConfiguration;
import com.linkedin.metadata.graph.EntityLineageResult;
import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.graph.LineageDirection;
import com.linkedin.metadata.graph.LineageRelationship;
import com.linkedin.metadata.graph.LineageRelationshipArray;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.models.registry.SnapshotEntityRegistry;
import com.linkedin.metadata.query.SearchFlags;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.aggregator.AllEntitiesSearchAggregator;
import com.linkedin.metadata.search.cache.CachingAllEntitiesSearchAggregator;
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
import com.linkedin.metadata.search.ranker.SimpleRanker;
import com.linkedin.metadata.search.utils.QueryUtils;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.metadata.utils.elasticsearch.IndexConventionImpl;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.elasticsearch.client.RestHighLevelClient;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.CacheManager;
import org.springframework.cache.concurrent.ConcurrentMapCacheManager;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.linkedin.metadata.ESTestConfiguration.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

@Import(ESTestConfiguration.class)
public class LineageSearchServiceTest extends AbstractTestNGSpringContextTests {

  @Autowired
  private RestHighLevelClient _searchClient;
  @Autowired
  private ESBulkProcessor _bulkProcessor;
  @Autowired
  private ESIndexBuilder _esIndexBuilder;

  private EntityRegistry _entityRegistry;
  private IndexConvention _indexConvention;
  private SettingsBuilder _settingsBuilder;
  private ElasticSearchService _elasticSearchService;
  private GraphService _graphService;
  private CacheManager _cacheManager;
  private LineageSearchService _lineageSearchService;

  private static final String ENTITY_NAME = "testEntity";
  private static final Urn TEST_URN = TestEntityUtil.getTestEntityUrn();
  private static final String TEST = "test";
  private static final String TEST1 = "test1";
  private static final Urn TEST_DATASET_URN = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,test,PROD)");

  @BeforeClass
  public void disableAssert() {
    PathSpecBasedSchemaAnnotationVisitor.class.getClassLoader()
        .setClassAssertionStatus(PathSpecBasedSchemaAnnotationVisitor.class.getName(), false);
  }

  @BeforeClass
  public void setup() {
    _entityRegistry = new SnapshotEntityRegistry(new Snapshot());
    _indexConvention = new IndexConventionImpl("lineage_search_service_test");
    _settingsBuilder = new SettingsBuilder(null);
    _elasticSearchService = buildEntitySearchService();
    _elasticSearchService.configure();
    _cacheManager = new ConcurrentMapCacheManager();
    _graphService = mock(GraphService.class);
    resetService(true);
  }

  private void resetService(boolean withCache) {
    CachingEntitySearchService cachingEntitySearchService = new CachingEntitySearchService(_cacheManager, _elasticSearchService, 100, true);
    EntityDocCountCacheConfiguration entityDocCountCacheConfiguration = new EntityDocCountCacheConfiguration();
    entityDocCountCacheConfiguration.setTtlSeconds(600L);
    _lineageSearchService = new LineageSearchService(
        new SearchService(
            new EntityDocCountCache(_entityRegistry, _elasticSearchService, entityDocCountCacheConfiguration),
            cachingEntitySearchService,
            new CachingAllEntitiesSearchAggregator(
                _cacheManager,
                new AllEntitiesSearchAggregator(_entityRegistry, _elasticSearchService, cachingEntitySearchService,
                    new SimpleRanker(), entityDocCountCacheConfiguration),
                100,
                true),
            new SimpleRanker()),
        _graphService, _cacheManager.getCache("test"), withCache);
  }

  @BeforeMethod
  public void wipe() throws Exception {
    _elasticSearchService.clear();
    clearCache();
    syncAfterWrite(_bulkProcessor);
  }

  @Nonnull
  private ElasticSearchService buildEntitySearchService() {
    EntityIndexBuilders indexBuilders =
        new EntityIndexBuilders(_esIndexBuilder, _entityRegistry,
            _indexConvention, _settingsBuilder);
    ESSearchDAO searchDAO = new ESSearchDAO(_entityRegistry, _searchClient, _indexConvention);
    ESBrowseDAO browseDAO = new ESBrowseDAO(_entityRegistry, _searchClient, _indexConvention);
    ESWriteDAO writeDAO = new ESWriteDAO(_entityRegistry, _searchClient, _indexConvention, _bulkProcessor, 1);
    return new ElasticSearchService(indexBuilders, searchDAO, browseDAO, writeDAO);
  }

  private void clearCache() {
    _cacheManager.getCacheNames().forEach(cache -> _cacheManager.getCache(cache).clear());
    resetService(true);
  }

  private EntityLineageResult mockResult(List<LineageRelationship> lineageRelationships) {
    return new EntityLineageResult().setRelationships(new LineageRelationshipArray(lineageRelationships))
        .setStart(0)
        .setCount(10)
        .setTotal(lineageRelationships.size());
  }

  @Test
  public void testSearchService() throws Exception {
    when(_graphService.getLineage(eq(TEST_URN), eq(LineageDirection.DOWNSTREAM), anyInt(), anyInt(),
        anyInt(), eq(null), eq(null))).thenReturn(mockResult(Collections.emptyList()));
    LineageSearchResult searchResult = searchAcrossLineage(null, TEST1);
    assertEquals(searchResult.getNumEntities().intValue(), 0);
    searchResult = searchAcrossLineage(null, TEST1);
    assertEquals(searchResult.getNumEntities().intValue(), 0);
    clearCache();

    when(_graphService.getLineage(eq(TEST_URN), eq(LineageDirection.DOWNSTREAM), anyInt(), anyInt(),
        anyInt(), eq(null), eq(null))).thenReturn(
        mockResult(ImmutableList.of(new LineageRelationship().setEntity(TEST_URN).setType("test").setDegree(1))));
    searchResult = searchAcrossLineage(null, TEST);
    assertEquals(searchResult.getNumEntities().intValue(), 0);
    searchResult = searchAcrossLineage(null, TEST1);
    assertEquals(searchResult.getNumEntities().intValue(), 0);
    clearCache();

    Urn urn = new TestEntityUrn("test1", "urn1", "VALUE_1");
    ObjectNode document = JsonNodeFactory.instance.objectNode();
    document.set("urn", JsonNodeFactory.instance.textNode(urn.toString()));
    document.set("keyPart1", JsonNodeFactory.instance.textNode("test"));
    document.set("textFieldOverride", JsonNodeFactory.instance.textNode("textFieldOverride"));
    document.set("browsePaths", JsonNodeFactory.instance.textNode("/a/b/c"));
    _elasticSearchService.upsertDocument(ENTITY_NAME, document.toString(), urn.toString());
    syncAfterWrite(_bulkProcessor);

    when(_graphService.getLineage(eq(TEST_URN), eq(LineageDirection.DOWNSTREAM), anyInt(), anyInt(),
        anyInt(), eq(null), eq(null))).thenReturn(mockResult(Collections.emptyList()));
    searchResult = searchAcrossLineage(null, TEST1);
    assertEquals(searchResult.getNumEntities().intValue(), 0);
    assertEquals(searchResult.getEntities().size(), 0);
    clearCache();

    when(_graphService.getLineage(eq(TEST_URN), eq(LineageDirection.DOWNSTREAM), anyInt(), anyInt(),
        anyInt(), eq(null), eq(null))).thenReturn(
        mockResult(ImmutableList.of(new LineageRelationship().setEntity(urn).setType("test").setDegree(1))));
    searchResult = searchAcrossLineage(null, TEST1);
    assertEquals(searchResult.getNumEntities().intValue(), 1);
    assertEquals(searchResult.getEntities().get(0).getEntity(), urn);
    assertEquals(searchResult.getEntities().get(0).getDegree().intValue(), 1);

    searchResult = searchAcrossLineage(QueryUtils.newFilter("degree.keyword", "1"), TEST1);
    assertEquals(searchResult.getNumEntities().intValue(), 1);
    assertEquals(searchResult.getEntities().get(0).getEntity(), urn);
    assertEquals(searchResult.getEntities().get(0).getDegree().intValue(), 1);

    searchResult = searchAcrossLineage(QueryUtils.newFilter("degree.keyword", "2"), TEST1);
    assertEquals(searchResult.getNumEntities().intValue(), 0);
    assertEquals(searchResult.getEntities().size(), 0);
    clearCache();

    Urn urn2 = new TestEntityUrn("test2", "urn2", "VALUE_2");
    ObjectNode document2 = JsonNodeFactory.instance.objectNode();
    document2.set("urn", JsonNodeFactory.instance.textNode(urn2.toString()));
    document2.set("keyPart1", JsonNodeFactory.instance.textNode("random"));
    document2.set("textFieldOverride", JsonNodeFactory.instance.textNode("textFieldOverride2"));
    document2.set("browsePaths", JsonNodeFactory.instance.textNode("/b/c"));
    _elasticSearchService.upsertDocument(ENTITY_NAME, document2.toString(), urn2.toString());
    syncAfterWrite(_bulkProcessor);

    searchResult = searchAcrossLineage(null, TEST1);
    assertEquals(searchResult.getNumEntities().intValue(), 1);
    assertEquals(searchResult.getEntities().get(0).getEntity(), urn);
    clearCache();

    when(_graphService.getLineage(eq(TEST_URN), eq(LineageDirection.DOWNSTREAM), anyInt(), anyInt(),
        anyInt(), eq(null), eq(null))).thenReturn(
        mockResult(ImmutableList.of(new LineageRelationship().setEntity(urn2).setType("test").setDegree(1))));
    searchResult = searchAcrossLineage(null, TEST1);
    assertEquals(searchResult.getNumEntities().intValue(), 0);
    assertEquals(searchResult.getEntities().size(), 0);
    clearCache();

    // Test Cache Behavior
    Mockito.reset(_graphService);

    // Case 1: Use the maxHops in the cache.
    when(_graphService.getLineage(eq(TEST_URN), eq(LineageDirection.DOWNSTREAM), anyInt(), anyInt(),
        eq(1000), eq(null), eq(null))).thenReturn(mockResult(
        ImmutableList.of(
            new LineageRelationship().setDegree(3).setType("type").setEntity(urn)
        )
    ));

    searchResult =
        _lineageSearchService.searchAcrossLineage(TEST_URN, LineageDirection.DOWNSTREAM, ImmutableList.of(ENTITY_NAME),
            "test1", 1000, null, null, 0, 10, null, null,
            new SearchFlags().setSkipCache(true));

    assertEquals(searchResult.getNumEntities().intValue(), 1);
    Mockito.verify(_graphService, times(1)).getLineage(eq(TEST_URN), eq(LineageDirection.DOWNSTREAM), anyInt(), anyInt(),
        eq(1000), eq(null), eq(null));

    // Hit the cache on second attempt
    searchResult = _lineageSearchService.searchAcrossLineage(TEST_URN, LineageDirection.DOWNSTREAM, ImmutableList.of(ENTITY_NAME),
        "test1", 1000, null, null, 0, 10, null, null,
        new SearchFlags().setSkipCache(true));
    assertEquals(searchResult.getNumEntities().intValue(), 1);
    Mockito.verify(_graphService, times(1)).getLineage(eq(TEST_URN), eq(LineageDirection.DOWNSTREAM), anyInt(), anyInt(),
        eq(1000), eq(null), eq(null));


    // Case 2: Use the start and end time in the cache.
    when(_graphService.getLineage(eq(TEST_URN), eq(LineageDirection.DOWNSTREAM), anyInt(), anyInt(),
        eq(1000), eq(0L), eq(1L))).thenReturn(mockResult(
        ImmutableList.of(
            new LineageRelationship().setDegree(3).setType("type").setEntity(urn)
        )
    ));

    searchResult =
        _lineageSearchService.searchAcrossLineage(TEST_URN, LineageDirection.DOWNSTREAM, ImmutableList.of(), "test1",
            null, null, null, 0, 10, 0L, 1L,
            new SearchFlags().setSkipCache(true));

    assertEquals(searchResult.getNumEntities().intValue(), 1);
    Mockito.verify(_graphService, times(1)).getLineage(eq(TEST_URN), eq(LineageDirection.DOWNSTREAM), anyInt(), anyInt(),
        eq(1000), eq(0L), eq(1L));

    // Hit the cache on second attempt
    searchResult = _lineageSearchService.searchAcrossLineage(TEST_URN, LineageDirection.DOWNSTREAM, ImmutableList.of(ENTITY_NAME),
        "test1", null, null, null, 0, 10, 0L, 1L,
        new SearchFlags().setSkipCache(true));
    assertEquals(searchResult.getNumEntities().intValue(), 1);
    Mockito.verify(_graphService, times(1)).getLineage(eq(TEST_URN), eq(LineageDirection.DOWNSTREAM), anyInt(), anyInt(),
        eq(1000), eq(0L), eq(1L));

    clearCache();

    // Cleanup
    _elasticSearchService.deleteDocument(ENTITY_NAME, urn.toString());
    _elasticSearchService.deleteDocument(ENTITY_NAME, urn2.toString());
    syncAfterWrite(_bulkProcessor);

    when(_graphService.getLineage(eq(TEST_URN), eq(LineageDirection.DOWNSTREAM), anyInt(), anyInt(),
        anyInt())).thenReturn(
        mockResult(ImmutableList.of(new LineageRelationship().setEntity(urn).setType("test1").setDegree(1))));
    searchResult = searchAcrossLineage(null, TEST1);

    assertEquals(searchResult.getNumEntities().intValue(), 0);

  }

  // Convenience method to reduce spots where we're sending the same params
  private LineageSearchResult searchAcrossLineage(@Nullable Filter filter, @Nonnull String input) {
    return _lineageSearchService.searchAcrossLineage(TEST_URN, LineageDirection.DOWNSTREAM, ImmutableList.of(), input,
        null, filter, null, 0, 10, null, null,
        new SearchFlags().setSkipCache(true));
  }
}
