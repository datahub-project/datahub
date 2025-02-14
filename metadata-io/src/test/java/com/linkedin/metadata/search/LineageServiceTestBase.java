package com.linkedin.metadata.search;

import static com.linkedin.metadata.Constants.DATASET_ENTITY_NAME;
import static com.linkedin.metadata.Constants.ELASTICSEARCH_IMPLEMENTATION_ELASTICSEARCH;
import static com.linkedin.metadata.utils.CriterionUtils.buildCriterion;
import static io.datahubproject.test.search.SearchTestUtils.syncAfterWrite;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.datahub.plugins.auth.authorization.Authorizer;
import com.datahub.test.Snapshot;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import com.linkedin.common.FabricType;
import com.linkedin.common.UrnArrayArray;
import com.linkedin.common.urn.DataPlatformUrn;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.common.urn.TestEntityUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.LongMap;
import com.linkedin.data.template.SetMode;
import com.linkedin.metadata.TestEntityUtil;
import com.linkedin.metadata.config.DataHubAppConfiguration;
import com.linkedin.metadata.config.MetadataChangeProposalConfig;
import com.linkedin.metadata.config.cache.CacheConfiguration;
import com.linkedin.metadata.config.cache.EntityDocCountCacheConfiguration;
import com.linkedin.metadata.config.cache.SearchCacheConfiguration;
import com.linkedin.metadata.config.cache.SearchLineageCacheConfiguration;
import com.linkedin.metadata.config.search.SearchConfiguration;
import com.linkedin.metadata.graph.EntityLineageResult;
import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.graph.LineageDirection;
import com.linkedin.metadata.graph.LineageRelationship;
import com.linkedin.metadata.graph.LineageRelationshipArray;
import com.linkedin.metadata.models.registry.SnapshotEntityRegistry;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
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
import com.linkedin.metadata.search.ranker.SimpleRanker;
import com.linkedin.metadata.search.utils.QueryUtils;
import com.linkedin.metadata.utils.elasticsearch.IndexConventionImpl;
import com.linkedin.r2.RemoteInvocationException;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RequestContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Getter;
import org.junit.Assert;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.client.RestHighLevelClient;
import org.springframework.cache.CacheManager;
import org.springframework.cache.concurrent.ConcurrentMapCacheManager;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public abstract class LineageServiceTestBase extends AbstractTestNGSpringContextTests {

  @Nonnull
  protected abstract RestHighLevelClient getSearchClient();

  @Nonnull
  protected abstract ESBulkProcessor getBulkProcessor();

  @Nonnull
  protected abstract ESIndexBuilder getIndexBuilder();

  @Nonnull
  protected abstract SearchConfiguration getSearchConfiguration();

  private SettingsBuilder settingsBuilder;
  private ElasticSearchService elasticSearchService;
  private GraphService graphService;
  private CacheManager cacheManager;
  private LineageSearchService lineageSearchService;
  private RestHighLevelClient searchClientSpy;

  @Getter private OperationContext operationContext;

  private static final String ENTITY_NAME = "testEntity";
  private static final Urn TEST_URN = TestEntityUtil.getTestEntityUrn();
  private static final String TEST = "test";
  private static final String TEST1 = "test1";
  private static final Urn TEST_DATASET_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,test,PROD)");

  @BeforeClass
  public void setup() throws RemoteInvocationException, URISyntaxException {
    operationContext =
        TestOperationContexts.systemContextNoSearchAuthorization(
                new SnapshotEntityRegistry(new Snapshot()),
                new IndexConventionImpl(
                    IndexConventionImpl.IndexConventionConfig.builder()
                        .prefix("lineage_search_service_test")
                        .hashIdAlgo("MD5")
                        .build()))
            .asSession(RequestContext.TEST, Authorizer.EMPTY, TestOperationContexts.TEST_USER_AUTH);
    settingsBuilder = new SettingsBuilder(null);
    elasticSearchService = buildEntitySearchService();
    elasticSearchService.reindexAll(Collections.emptySet());
    cacheManager = new ConcurrentMapCacheManager();
    graphService = mock(GraphService.class);
    resetService(true, false);
  }

  private void resetService(boolean withCache, boolean withLightingCache) {
    CachingEntitySearchService cachingEntitySearchService =
        new CachingEntitySearchService(cacheManager, elasticSearchService, 100, true);
    EntityDocCountCacheConfiguration entityDocCountCacheConfiguration =
        new EntityDocCountCacheConfiguration();
    entityDocCountCacheConfiguration.setTtlSeconds(600L);

    DataHubAppConfiguration appConfig = new DataHubAppConfiguration();
    appConfig.setCache(new CacheConfiguration());
    appConfig.getCache().setSearch(new SearchCacheConfiguration());
    appConfig.getCache().getSearch().setLineage(new SearchLineageCacheConfiguration());
    appConfig.getCache().getSearch().getLineage().setTtlSeconds(600L);
    appConfig
        .getCache()
        .getSearch()
        .getLineage()
        .setLightningThreshold(withLightingCache ? -1 : 300);
    appConfig.setMetadataChangeProposal(new MetadataChangeProposalConfig());
    appConfig
        .getMetadataChangeProposal()
        .setSideEffects(new MetadataChangeProposalConfig.SideEffectsConfig());
    appConfig
        .getMetadataChangeProposal()
        .getSideEffects()
        .setSchemaField(new MetadataChangeProposalConfig.SideEffectConfig());
    appConfig.getMetadataChangeProposal().getSideEffects().getSchemaField().setEnabled(false);

    lineageSearchService =
        spy(
            new LineageSearchService(
                new SearchService(
                    new EntityDocCountCache(
                        operationContext.getEntityRegistry(),
                        elasticSearchService,
                        entityDocCountCacheConfiguration),
                    cachingEntitySearchService,
                    new SimpleRanker()),
                graphService,
                cacheManager.getCache("test"),
                withCache,
                appConfig));
  }

  @BeforeMethod
  public void wipe() throws Exception {
    syncAfterWrite(getBulkProcessor());
    elasticSearchService.clear(operationContext);
    clearCache(false);
    syncAfterWrite(getBulkProcessor());
  }

  @Nonnull
  private ElasticSearchService buildEntitySearchService() {
    EntityIndexBuilders indexBuilders =
        new EntityIndexBuilders(
            getIndexBuilder(),
            operationContext.getEntityRegistry(),
            operationContext.getSearchContext().getIndexConvention(),
            settingsBuilder);
    searchClientSpy = spy(getSearchClient());
    ESSearchDAO searchDAO =
        new ESSearchDAO(
            searchClientSpy,
            false,
            ELASTICSEARCH_IMPLEMENTATION_ELASTICSEARCH,
            getSearchConfiguration(),
            null,
            QueryFilterRewriteChain.EMPTY);
    ESBrowseDAO browseDAO =
        new ESBrowseDAO(
            searchClientSpy, getSearchConfiguration(), null, QueryFilterRewriteChain.EMPTY);
    ESWriteDAO writeDAO = new ESWriteDAO(searchClientSpy, getBulkProcessor(), 1);
    return new ElasticSearchService(indexBuilders, searchDAO, browseDAO, writeDAO);
  }

  private void clearCache(boolean withLightingCache) {
    cacheManager.getCacheNames().forEach(cache -> cacheManager.getCache(cache).clear());
    resetService(true, withLightingCache);
  }

  private EntityLineageResult mockResult(List<LineageRelationship> lineageRelationships) {
    return new EntityLineageResult()
        .setRelationships(new LineageRelationshipArray(lineageRelationships))
        .setStart(0)
        .setCount(10)
        .setTotal(lineageRelationships.size());
  }

  @Test
  public void testSearchService() throws Exception {
    when(graphService.getLineage(
            eq(getOperationContext().withSearchFlags(f -> f.setSkipCache(true))),
            eq(TEST_URN),
            eq(LineageDirection.DOWNSTREAM),
            anyInt(),
            anyInt(),
            anyInt()))
        .thenReturn(mockResult(Collections.emptyList()));
    LineageSearchResult searchResult = searchAcrossLineage(null, TEST1);
    assertEquals(searchResult.getNumEntities().intValue(), 0);
    searchResult = searchAcrossLineage(null, TEST1);
    assertEquals(searchResult.getNumEntities().intValue(), 0);
    clearCache(false);

    when(graphService.getLineage(
            eq(getOperationContext().withSearchFlags(f -> f.setSkipCache(true))),
            eq(TEST_URN),
            eq(LineageDirection.DOWNSTREAM),
            anyInt(),
            anyInt(),
            anyInt()))
        .thenReturn(
            mockResult(
                ImmutableList.of(
                    new LineageRelationship().setEntity(TEST_URN).setType("test").setDegree(1))));
    // just testing null input does not throw any exception
    searchAcrossLineage(null, null);

    searchResult = searchAcrossLineage(null, TEST);
    assertEquals(searchResult.getNumEntities().intValue(), 0);
    searchResult = searchAcrossLineage(null, TEST1);
    assertEquals(searchResult.getNumEntities().intValue(), 0);
    clearCache(false);

    Urn urn = new TestEntityUrn("test1", "urn1", "VALUE_1");
    ObjectNode document = JsonNodeFactory.instance.objectNode();
    document.set("urn", JsonNodeFactory.instance.textNode(urn.toString()));
    document.set("keyPart1", JsonNodeFactory.instance.textNode("test"));
    document.set("textFieldOverride", JsonNodeFactory.instance.textNode("textFieldOverride"));
    document.set("browsePaths", JsonNodeFactory.instance.textNode("/a/b/c"));
    elasticSearchService.upsertDocument(
        getOperationContext(), ENTITY_NAME, document.toString(), urn.toString());
    syncAfterWrite(getBulkProcessor());

    when(graphService.getLineage(
            eq(getOperationContext().withSearchFlags(f -> f.setSkipCache(true))),
            eq(TEST_URN),
            eq(LineageDirection.DOWNSTREAM),
            anyInt(),
            anyInt(),
            anyInt()))
        .thenReturn(mockResult(Collections.emptyList()));
    searchResult = searchAcrossLineage(null, TEST1);
    assertEquals(searchResult.getNumEntities().intValue(), 0);
    assertEquals(searchResult.getEntities().size(), 0);
    clearCache(false);

    when(graphService.getLineage(
            eq(getOperationContext().withSearchFlags(f -> f.setSkipCache(true))),
            eq(TEST_URN),
            eq(LineageDirection.DOWNSTREAM),
            anyInt(),
            anyInt(),
            anyInt()))
        .thenReturn(
            mockResult(
                ImmutableList.of(
                    new LineageRelationship().setEntity(urn).setType("test").setDegree(1))));
    searchResult = searchAcrossLineage(null, TEST1);
    assertEquals(searchResult.getNumEntities().intValue(), 1);
    assertEquals(searchResult.getEntities().get(0).getEntity(), urn);
    assertEquals(searchResult.getEntities().get(0).getDegree().intValue(), 1);

    searchResult = searchAcrossLineage(QueryUtils.newFilter("degree", "1"), TEST1);
    assertEquals(searchResult.getNumEntities().intValue(), 1);
    assertEquals(searchResult.getEntities().get(0).getEntity(), urn);
    assertEquals(searchResult.getEntities().get(0).getDegree().intValue(), 1);

    searchResult = searchAcrossLineage(QueryUtils.newFilter("degree", "2"), TEST1);
    assertEquals(searchResult.getNumEntities().intValue(), 0);
    assertEquals(searchResult.getEntities().size(), 0);
    clearCache(false);

    Urn urn2 = new TestEntityUrn("test2", "urn2", "VALUE_2");
    ObjectNode document2 = JsonNodeFactory.instance.objectNode();
    document2.set("urn", JsonNodeFactory.instance.textNode(urn2.toString()));
    document2.set("keyPart1", JsonNodeFactory.instance.textNode("random"));
    document2.set("textFieldOverride", JsonNodeFactory.instance.textNode("textFieldOverride2"));
    document2.set("browsePaths", JsonNodeFactory.instance.textNode("/b/c"));
    elasticSearchService.upsertDocument(
        getOperationContext(), ENTITY_NAME, document2.toString(), urn2.toString());
    syncAfterWrite(getBulkProcessor());

    Mockito.reset(searchClientSpy);
    searchResult = searchAcrossLineage(null, TEST1);
    assertEquals(searchResult.getNumEntities().intValue(), 1);
    assertEquals(searchResult.getEntities().get(0).getEntity(), urn);
    // Verify that highlighting was turned off in the query
    ArgumentCaptor<SearchRequest> searchRequestCaptor =
        ArgumentCaptor.forClass(SearchRequest.class);
    Mockito.verify(searchClientSpy, times(1)).search(searchRequestCaptor.capture(), any());
    SearchRequest capturedRequest = searchRequestCaptor.getValue();
    assertNull(capturedRequest.source().highlighter());
    clearCache(false);

    when(graphService.getLineage(
            eq(getOperationContext().withSearchFlags(f -> f.setSkipCache(true))),
            eq(TEST_URN),
            eq(LineageDirection.DOWNSTREAM),
            anyInt(),
            anyInt(),
            anyInt()))
        .thenReturn(
            mockResult(
                ImmutableList.of(
                    new LineageRelationship().setEntity(urn2).setType("test").setDegree(1))));
    searchResult = searchAcrossLineage(null, TEST1);
    assertEquals(searchResult.getNumEntities().intValue(), 0);
    assertEquals(searchResult.getEntities().size(), 0);
    clearCache(false);

    // Test Cache Behavior
    Mockito.reset(graphService);

    // Case 1: Use the maxHops in the cache.
    when(graphService.getLineage(
            eq(getOperationContext()),
            eq(TEST_URN),
            eq(LineageDirection.DOWNSTREAM),
            anyInt(),
            anyInt(),
            eq(1000)))
        .thenReturn(
            mockResult(
                ImmutableList.of(
                    new LineageRelationship().setDegree(3).setType("type").setEntity(urn))));

    searchResult =
        lineageSearchService.searchAcrossLineage(
            getOperationContext(),
            TEST_URN,
            LineageDirection.DOWNSTREAM,
            ImmutableList.of(ENTITY_NAME),
            "test1",
            1000,
            null,
            null,
            0,
            10);

    assertEquals(searchResult.getNumEntities().intValue(), 1);
    Mockito.verify(graphService, times(1))
        .getLineage(
            eq(getOperationContext()),
            eq(TEST_URN),
            eq(LineageDirection.DOWNSTREAM),
            anyInt(),
            anyInt(),
            eq(1000));

    // Hit the cache on second attempt
    searchResult =
        lineageSearchService.searchAcrossLineage(
            getOperationContext(),
            TEST_URN,
            LineageDirection.DOWNSTREAM,
            ImmutableList.of(ENTITY_NAME),
            "test1",
            1000,
            null,
            null,
            0,
            10);
    assertEquals(searchResult.getNumEntities().intValue(), 1);
    Mockito.verify(graphService, times(1))
        .getLineage(
            eq(getOperationContext()),
            eq(TEST_URN),
            eq(LineageDirection.DOWNSTREAM),
            anyInt(),
            anyInt(),
            eq(1000));

    // Case 2: Use the start and end time in the cache.
    when(graphService.getLineage(
            eq(
                getOperationContext()
                    .withLineageFlags(f -> f.setStartTimeMillis(0L).setEndTimeMillis(1L))),
            eq(TEST_URN),
            eq(LineageDirection.DOWNSTREAM),
            anyInt(),
            anyInt(),
            eq(1000)))
        .thenReturn(
            mockResult(
                ImmutableList.of(
                    new LineageRelationship().setDegree(3).setType("type").setEntity(urn))));

    searchResult =
        lineageSearchService.searchAcrossLineage(
            getOperationContext()
                .withLineageFlags(flags -> flags.setStartTimeMillis(0L).setEndTimeMillis(1L)),
            TEST_URN,
            LineageDirection.DOWNSTREAM,
            ImmutableList.of(),
            "test1",
            null,
            null,
            null,
            0,
            10);

    assertEquals(searchResult.getNumEntities().intValue(), 1);
    Mockito.verify(graphService, times(1))
        .getLineage(
            eq(
                getOperationContext()
                    .withLineageFlags(f -> f.setStartTimeMillis(0L).setEndTimeMillis(1L))),
            eq(TEST_URN),
            eq(LineageDirection.DOWNSTREAM),
            anyInt(),
            anyInt(),
            eq(1000));

    // Hit the cache on second attempt
    searchResult =
        lineageSearchService.searchAcrossLineage(
            getOperationContext()
                .withLineageFlags(flags -> flags.setStartTimeMillis(0L).setEndTimeMillis(1L)),
            TEST_URN,
            LineageDirection.DOWNSTREAM,
            ImmutableList.of(ENTITY_NAME),
            "test1",
            null,
            null,
            null,
            0,
            10);
    assertEquals(searchResult.getNumEntities().intValue(), 1);
    Mockito.verify(graphService, times(1))
        .getLineage(
            eq(
                getOperationContext()
                    .withLineageFlags(f -> f.setStartTimeMillis(0L).setEndTimeMillis(1L))),
            eq(TEST_URN),
            eq(LineageDirection.DOWNSTREAM),
            anyInt(),
            anyInt(),
            eq(1000));

    clearCache(false);

    // Cleanup
    elasticSearchService.deleteDocument(getOperationContext(), ENTITY_NAME, urn.toString());
    elasticSearchService.deleteDocument(getOperationContext(), ENTITY_NAME, urn2.toString());
    syncAfterWrite(getBulkProcessor());

    when(graphService.getLineage(
            eq(getOperationContext().withSearchFlags(f -> f.setSkipCache(true))),
            eq(TEST_URN),
            eq(LineageDirection.DOWNSTREAM),
            anyInt(),
            anyInt(),
            anyInt()))
        .thenReturn(
            mockResult(
                ImmutableList.of(
                    new LineageRelationship().setEntity(urn).setType("test1").setDegree(1))));
    searchResult = searchAcrossLineage(null, TEST1);

    assertEquals(searchResult.getNumEntities().intValue(), 0);
  }

  @Test
  public void testScrollAcrossLineage() throws Exception {
    when(graphService.getLineage(
            eq(getOperationContext().withSearchFlags(f -> f.setSkipCache(true))),
            eq(TEST_URN),
            eq(LineageDirection.DOWNSTREAM),
            anyInt(),
            anyInt(),
            anyInt()))
        .thenReturn(mockResult(Collections.emptyList()));
    LineageScrollResult scrollResult = scrollAcrossLineage(null, TEST1);
    assertEquals(scrollResult.getNumEntities().intValue(), 0);
    assertNull(scrollResult.getScrollId());
    scrollResult = scrollAcrossLineage(null, TEST1);
    assertEquals(scrollResult.getNumEntities().intValue(), 0);
    assertNull(scrollResult.getScrollId());
    clearCache(false);

    when(graphService.getLineage(
            eq(getOperationContext().withSearchFlags(f -> f.setSkipCache(true))),
            eq(TEST_URN),
            eq(LineageDirection.DOWNSTREAM),
            anyInt(),
            anyInt(),
            anyInt()))
        .thenReturn(
            mockResult(
                ImmutableList.of(
                    new LineageRelationship().setEntity(TEST_URN).setType("test").setDegree(1))));
    // just testing null input does not throw any exception
    scrollAcrossLineage(null, null);

    scrollResult = scrollAcrossLineage(null, TEST);
    assertEquals(scrollResult.getNumEntities().intValue(), 0);
    assertNull(scrollResult.getScrollId());
    scrollResult = scrollAcrossLineage(null, TEST1);
    assertEquals(scrollResult.getNumEntities().intValue(), 0);
    assertNull(scrollResult.getScrollId());
    clearCache(false);

    Urn urn = new TestEntityUrn("test1", "urn1", "VALUE_1");
    ObjectNode document = JsonNodeFactory.instance.objectNode();
    document.set("urn", JsonNodeFactory.instance.textNode(urn.toString()));
    document.set("keyPart1", JsonNodeFactory.instance.textNode("test"));
    document.set("textFieldOverride", JsonNodeFactory.instance.textNode("textFieldOverride"));
    document.set("browsePaths", JsonNodeFactory.instance.textNode("/a/b/c"));
    elasticSearchService.upsertDocument(
        operationContext, ENTITY_NAME, document.toString(), urn.toString());
    syncAfterWrite(getBulkProcessor());

    when(graphService.getLineage(
            eq(getOperationContext().withSearchFlags(f -> f.setSkipCache(true))),
            eq(TEST_URN),
            eq(LineageDirection.DOWNSTREAM),
            anyInt(),
            anyInt(),
            anyInt()))
        .thenReturn(mockResult(Collections.emptyList()));
    scrollResult = scrollAcrossLineage(null, TEST1);
    assertEquals(scrollResult.getNumEntities().intValue(), 0);
    assertEquals(scrollResult.getEntities().size(), 0);
    assertNull(scrollResult.getScrollId());
    clearCache(false);

    when(graphService.getLineage(
            eq(getOperationContext().withSearchFlags(f -> f.setSkipCache(true))),
            eq(TEST_URN),
            eq(LineageDirection.DOWNSTREAM),
            anyInt(),
            anyInt(),
            anyInt()))
        .thenReturn(
            mockResult(
                ImmutableList.of(
                    new LineageRelationship().setEntity(urn).setType("test").setDegree(1))));
    scrollResult = scrollAcrossLineage(null, TEST1);
    assertEquals(scrollResult.getNumEntities().intValue(), 1);
    assertEquals(scrollResult.getEntities().get(0).getEntity(), urn);
    assertEquals(scrollResult.getEntities().get(0).getDegree().intValue(), 1);
    assertNull(scrollResult.getScrollId());

    scrollResult = scrollAcrossLineage(QueryUtils.newFilter("degree", "1"), TEST1);
    assertEquals(scrollResult.getNumEntities().intValue(), 1);
    assertEquals(scrollResult.getEntities().get(0).getEntity(), urn);
    assertEquals(scrollResult.getEntities().get(0).getDegree().intValue(), 1);
    assertNull(scrollResult.getScrollId());

    scrollResult = scrollAcrossLineage(QueryUtils.newFilter("degree", "2"), TEST1);
    assertEquals(scrollResult.getNumEntities().intValue(), 0);
    assertEquals(scrollResult.getEntities().size(), 0);
    assertNull(scrollResult.getScrollId());
    clearCache(false);

    // Cleanup
    elasticSearchService.deleteDocument(getOperationContext(), ENTITY_NAME, urn.toString());
    syncAfterWrite(getBulkProcessor());

    when(graphService.getLineage(
            eq(getOperationContext().withSearchFlags(f -> f.setSkipCache(true))),
            eq(TEST_URN),
            eq(LineageDirection.DOWNSTREAM),
            anyInt(),
            anyInt(),
            anyInt()))
        .thenReturn(
            mockResult(
                ImmutableList.of(
                    new LineageRelationship().setEntity(urn).setType("test1").setDegree(1))));
    scrollResult = scrollAcrossLineage(null, TEST1);

    assertEquals(scrollResult.getNumEntities().intValue(), 0);
    assertNull(scrollResult.getScrollId());
  }

  @Test
  public void testLightningSearchService() throws Exception {
    // Mostly this test ensures the code path is exercised

    // Lightning depends on star/empty/null
    final String testStar = "*";

    // Enable lightning
    resetService(true, true);

    when(graphService.getLineage(
            eq(getOperationContext().withSearchFlags(f -> f.setSkipCache(true))),
            eq(TEST_URN),
            eq(LineageDirection.DOWNSTREAM),
            anyInt(),
            anyInt(),
            anyInt()))
        .thenReturn(mockResult(Collections.emptyList()));
    LineageSearchResult searchResult = searchAcrossLineage(null, testStar);
    assertEquals(searchResult.getNumEntities().intValue(), 0);
    clearCache(true);

    when(graphService.getLineage(
            eq(getOperationContext().withSearchFlags(f -> f.setSkipCache(true))),
            eq(TEST_URN),
            eq(LineageDirection.DOWNSTREAM),
            anyInt(),
            anyInt(),
            anyInt()))
        .thenReturn(
            mockResult(
                ImmutableList.of(
                    new LineageRelationship().setEntity(TEST_URN).setType("test").setDegree(1))));
    searchResult = searchAcrossLineage(null, testStar);
    assertEquals(searchResult.getNumEntities().intValue(), 1);
    clearCache(true);

    Urn urn = new TestEntityUrn("test1", "urn1", "VALUE_1");
    ObjectNode document = JsonNodeFactory.instance.objectNode();
    document.set("urn", JsonNodeFactory.instance.textNode(urn.toString()));
    document.set("keyPart1", JsonNodeFactory.instance.textNode("test"));
    document.set("textFieldOverride", JsonNodeFactory.instance.textNode("textFieldOverride"));
    document.set("browsePaths", JsonNodeFactory.instance.textNode("/a/b/c"));
    elasticSearchService.upsertDocument(
        getOperationContext(), ENTITY_NAME, document.toString(), urn.toString());
    syncAfterWrite(getBulkProcessor());

    when(graphService.getLineage(
            eq(getOperationContext().withSearchFlags(f -> f.setSkipCache(true))),
            eq(TEST_URN),
            eq(LineageDirection.DOWNSTREAM),
            anyInt(),
            anyInt(),
            anyInt()))
        .thenReturn(mockResult(Collections.emptyList()));
    searchResult = searchAcrossLineage(null, testStar);
    assertEquals(searchResult.getNumEntities().intValue(), 0);
    assertEquals(searchResult.getEntities().size(), 0);
    clearCache(true);

    when(graphService.getLineage(
            eq(getOperationContext().withSearchFlags(f -> f.setSkipCache(true))),
            eq(TEST_URN),
            eq(LineageDirection.DOWNSTREAM),
            anyInt(),
            anyInt(),
            anyInt()))
        .thenReturn(
            mockResult(
                ImmutableList.of(
                    new LineageRelationship().setEntity(urn).setType("test").setDegree(1))));
    searchResult = searchAcrossLineage(null, testStar);
    assertEquals(searchResult.getNumEntities().intValue(), 1);
    assertEquals(searchResult.getEntities().get(0).getEntity(), urn);
    assertEquals(searchResult.getEntities().get(0).getDegree().intValue(), 1);
    verify(lineageSearchService, times(1))
        .getLightningSearchResult(any(), any(), anyInt(), anyInt(), anySet());

    searchResult = searchAcrossLineage(QueryUtils.newFilter("degree", "1"), testStar);
    assertEquals(searchResult.getNumEntities().intValue(), 1);
    assertEquals(searchResult.getEntities().get(0).getEntity(), urn);
    assertEquals(searchResult.getEntities().get(0).getDegree().intValue(), 1);
    verify(lineageSearchService, times(2))
        .getLightningSearchResult(any(), any(), anyInt(), anyInt(), anySet());

    searchResult = searchAcrossLineage(QueryUtils.newFilter("degree", "2"), testStar);
    assertEquals(searchResult.getNumEntities().intValue(), 0);
    assertEquals(searchResult.getEntities().size(), 0);
    verify(lineageSearchService, times(3))
        .getLightningSearchResult(any(), any(), anyInt(), anyInt(), anySet());
    clearCache(true); // resets spy

    Urn urn2 = new TestEntityUrn("test2", "urn2", "VALUE_2");
    ObjectNode document2 = JsonNodeFactory.instance.objectNode();
    document2.set("urn", JsonNodeFactory.instance.textNode(urn2.toString()));
    document2.set("keyPart1", JsonNodeFactory.instance.textNode("random"));
    document2.set("textFieldOverride", JsonNodeFactory.instance.textNode("textFieldOverride2"));
    document2.set("browsePaths", JsonNodeFactory.instance.textNode("/b/c"));
    elasticSearchService.upsertDocument(
        getOperationContext(), ENTITY_NAME, document2.toString(), urn2.toString());
    syncAfterWrite(getBulkProcessor());

    searchResult = searchAcrossLineage(null, testStar);
    assertEquals(searchResult.getNumEntities().intValue(), 1);
    assertEquals(searchResult.getEntities().get(0).getEntity(), urn);
    verify(lineageSearchService, times(1))
        .getLightningSearchResult(any(), any(), anyInt(), anyInt(), anySet());
    clearCache(true);

    when(graphService.getLineage(
            eq(getOperationContext().withSearchFlags(f -> f.setSkipCache(true))),
            eq(TEST_URN),
            eq(LineageDirection.DOWNSTREAM),
            anyInt(),
            anyInt(),
            anyInt()))
        .thenReturn(
            mockResult(
                ImmutableList.of(
                    new LineageRelationship().setEntity(urn2).setType("test").setDegree(1))));
    searchResult = searchAcrossLineage(null, testStar);
    assertEquals(searchResult.getNumEntities().intValue(), 1);
    assertEquals(searchResult.getEntities().size(), 1);
    verify(lineageSearchService, times(1))
        .getLightningSearchResult(any(), any(), anyInt(), anyInt(), anySet());
    clearCache(true);

    // Test Cache Behavior
    reset(graphService);
    reset(lineageSearchService);

    // Case 1: Use the maxHops in the cache.
    when(graphService.getLineage(
            eq(getOperationContext()),
            eq(TEST_URN),
            eq(LineageDirection.DOWNSTREAM),
            anyInt(),
            anyInt(),
            eq(1000)))
        .thenReturn(
            mockResult(
                ImmutableList.of(
                    new LineageRelationship().setDegree(3).setType("type").setEntity(urn))));

    searchResult =
        lineageSearchService.searchAcrossLineage(
            getOperationContext(),
            TEST_URN,
            LineageDirection.DOWNSTREAM,
            ImmutableList.of(ENTITY_NAME),
            "*",
            1000,
            null,
            null,
            0,
            10);

    assertEquals(searchResult.getNumEntities().intValue(), 1);
    verify(graphService, times(1))
        .getLineage(
            eq(getOperationContext()),
            eq(TEST_URN),
            eq(LineageDirection.DOWNSTREAM),
            anyInt(),
            anyInt(),
            eq(1000));
    verify(lineageSearchService, times(1))
        .getLightningSearchResult(any(), any(), anyInt(), anyInt(), anySet());

    // Hit the cache on second attempt
    searchResult =
        lineageSearchService.searchAcrossLineage(
            getOperationContext(),
            TEST_URN,
            LineageDirection.DOWNSTREAM,
            ImmutableList.of(ENTITY_NAME),
            "*",
            1000,
            null,
            null,
            0,
            10);
    assertEquals(searchResult.getNumEntities().intValue(), 1);
    verify(graphService, times(1))
        .getLineage(
            eq(getOperationContext()),
            eq(TEST_URN),
            eq(LineageDirection.DOWNSTREAM),
            anyInt(),
            anyInt(),
            eq(1000));
    verify(lineageSearchService, times(2))
        .getLightningSearchResult(any(), any(), anyInt(), anyInt(), anySet());

    // Case 2: Use the start and end time in the cache.
    when(graphService.getLineage(
            eq(
                getOperationContext()
                    .withLineageFlags(f -> f.setStartTimeMillis(0L).setEndTimeMillis(1L))),
            eq(TEST_URN),
            eq(LineageDirection.DOWNSTREAM),
            anyInt(),
            anyInt(),
            eq(1000)))
        .thenReturn(
            mockResult(
                ImmutableList.of(
                    new LineageRelationship().setDegree(3).setType("type").setEntity(urn))));

    searchResult =
        lineageSearchService.searchAcrossLineage(
            getOperationContext()
                .withLineageFlags(flags -> flags.setStartTimeMillis(0L).setEndTimeMillis(1L)),
            TEST_URN,
            LineageDirection.DOWNSTREAM,
            ImmutableList.of(),
            "*",
            null,
            null,
            null,
            0,
            10);

    assertEquals(searchResult.getNumEntities().intValue(), 1);
    String invocations = mockingDetails(graphService).printInvocations();
    System.out.println(invocations);
    verify(graphService, times(1))
        .getLineage(
            eq(
                getOperationContext()
                    .withLineageFlags(f -> f.setStartTimeMillis(0L).setEndTimeMillis(1L))),
            eq(TEST_URN),
            eq(LineageDirection.DOWNSTREAM),
            anyInt(),
            anyInt(),
            eq(1000));
    verify(lineageSearchService, times(3))
        .getLightningSearchResult(any(), any(), anyInt(), anyInt(), anySet());

    // Hit the cache on second attempt
    searchResult =
        lineageSearchService.searchAcrossLineage(
            getOperationContext()
                .withLineageFlags(flags -> flags.setStartTimeMillis(0L).setEndTimeMillis(1L)),
            TEST_URN,
            LineageDirection.DOWNSTREAM,
            ImmutableList.of(ENTITY_NAME),
            "*",
            null,
            null,
            null,
            0,
            10);
    assertEquals(searchResult.getNumEntities().intValue(), 1);
    verify(graphService, times(1))
        .getLineage(
            eq(
                getOperationContext()
                    .withLineageFlags(f -> f.setStartTimeMillis(0L).setEndTimeMillis(1L))),
            eq(TEST_URN),
            eq(LineageDirection.DOWNSTREAM),
            anyInt(),
            anyInt(),
            eq(1000));
    verify(lineageSearchService, times(4))
        .getLightningSearchResult(any(), any(), anyInt(), anyInt(), anySet());

    /*
     * Test filtering
     */
    reset(lineageSearchService);

    // Entity
    searchResult =
        lineageSearchService.searchAcrossLineage(
            getOperationContext(),
            TEST_URN,
            LineageDirection.DOWNSTREAM,
            ImmutableList.of(DATASET_ENTITY_NAME),
            "*",
            1000,
            null,
            null,
            0,
            10);
    assertEquals(searchResult.getNumEntities().intValue(), 0);
    assertEquals(searchResult.getEntities().size(), 0);
    verify(lineageSearchService, times(1))
        .getLightningSearchResult(any(), any(), anyInt(), anyInt(), anySet());

    // Cached
    searchResult =
        lineageSearchService.searchAcrossLineage(
            getOperationContext()
                .withLineageFlags(flags -> flags.setStartTimeMillis(0L).setEndTimeMillis(1L)),
            TEST_URN,
            LineageDirection.DOWNSTREAM,
            ImmutableList.of(DATASET_ENTITY_NAME),
            "*",
            1000,
            null,
            null,
            0,
            10);
    Mockito.verify(graphService, times(1))
        .getLineage(
            eq(
                operationContext.withLineageFlags(
                    f -> f.setStartTimeMillis(0L).setEndTimeMillis(1L))),
            eq(TEST_URN),
            eq(LineageDirection.DOWNSTREAM),
            anyInt(),
            anyInt(),
            eq(1000));
    verify(lineageSearchService, times(2))
        .getLightningSearchResult(any(), any(), anyInt(), anyInt(), anySet());
    assertEquals(searchResult.getNumEntities().intValue(), 0);
    assertEquals(searchResult.getEntities().size(), 0);

    // Platform
    ConjunctiveCriterionArray conCritArr = new ConjunctiveCriterionArray();
    Criterion platform1Crit =
        buildCriterion("platform", Condition.EQUAL, "urn:li:dataPlatform:kafka");
    CriterionArray critArr = new CriterionArray(ImmutableList.of(platform1Crit));
    conCritArr.add(new ConjunctiveCriterion().setAnd(critArr));
    Criterion degreeCrit = buildCriterion("degree", Condition.EQUAL, "2");
    conCritArr.add(
        new ConjunctiveCriterion().setAnd(new CriterionArray(ImmutableList.of(degreeCrit))));
    Filter filter = new Filter().setOr(conCritArr);

    searchResult =
        lineageSearchService.searchAcrossLineage(
            getOperationContext(),
            TEST_URN,
            LineageDirection.DOWNSTREAM,
            ImmutableList.of(ENTITY_NAME),
            "*",
            1000,
            filter,
            null,
            0,
            10);
    assertEquals(searchResult.getNumEntities().intValue(), 0);
    assertEquals(searchResult.getEntities().size(), 0);
    verify(lineageSearchService, times(3))
        .getLightningSearchResult(any(), any(), anyInt(), anyInt(), anySet());

    // Cached
    searchResult =
        lineageSearchService.searchAcrossLineage(
            getOperationContext(),
            TEST_URN,
            LineageDirection.DOWNSTREAM,
            ImmutableList.of(ENTITY_NAME),
            "*",
            1000,
            filter,
            null,
            0,
            10);
    verify(graphService, times(1))
        .getLineage(
            eq(getOperationContext()),
            eq(TEST_URN),
            eq(LineageDirection.DOWNSTREAM),
            anyInt(),
            anyInt(),
            eq(1000));
    verify(lineageSearchService, times(4))
        .getLightningSearchResult(any(), any(), anyInt(), anyInt(), anySet());
    assertEquals(searchResult.getNumEntities().intValue(), 0);
    assertEquals(searchResult.getEntities().size(), 0);

    // Environment
    Filter originFilter = QueryUtils.newFilter("origin", "PROD");
    searchResult =
        lineageSearchService.searchAcrossLineage(
            getOperationContext(),
            TEST_URN,
            LineageDirection.DOWNSTREAM,
            ImmutableList.of(ENTITY_NAME),
            "*",
            1000,
            originFilter,
            null,
            0,
            10);
    assertEquals(searchResult.getNumEntities().intValue(), 0);
    assertEquals(searchResult.getEntities().size(), 0);
    verify(lineageSearchService, times(5))
        .getLightningSearchResult(any(), any(), anyInt(), anyInt(), anySet());

    // Cached
    searchResult =
        lineageSearchService.searchAcrossLineage(
            getOperationContext()
                .withLineageFlags(flags -> flags.setStartTimeMillis(0L).setEndTimeMillis(1L)),
            TEST_URN,
            LineageDirection.DOWNSTREAM,
            ImmutableList.of(ENTITY_NAME),
            "*",
            1000,
            originFilter,
            null,
            0,
            10);
    verify(graphService, times(1))
        .getLineage(
            eq(
                getOperationContext()
                    .withLineageFlags(f -> f.setStartTimeMillis(0L).setEndTimeMillis(1L))),
            eq(TEST_URN),
            eq(LineageDirection.DOWNSTREAM),
            anyInt(),
            anyInt(),
            eq(1000));
    verify(lineageSearchService, times(6))
        .getLightningSearchResult(any(), any(), anyInt(), anyInt(), anySet());
    assertEquals(searchResult.getNumEntities().intValue(), 0);
    assertEquals(searchResult.getEntities().size(), 0);

    clearCache(true);

    // Cleanup
    elasticSearchService.deleteDocument(getOperationContext(), ENTITY_NAME, urn.toString());
    elasticSearchService.deleteDocument(getOperationContext(), ENTITY_NAME, urn2.toString());
    syncAfterWrite(getBulkProcessor());

    when(graphService.getLineage(
            eq(getOperationContext().withSearchFlags(f -> f.setSkipCache(true))),
            eq(TEST_URN),
            eq(LineageDirection.DOWNSTREAM),
            anyInt(),
            anyInt(),
            anyInt()))
        .thenReturn(
            mockResult(
                ImmutableList.of(
                    new LineageRelationship().setEntity(urn).setType("test1").setDegree(1))));
    searchResult = searchAcrossLineage(null, testStar);

    assertEquals(searchResult.getNumEntities().intValue(), 1);
  }

  @Test
  public void testLightningEnvFiltering() throws Exception {
    Map<String, Integer> platformCounts = new HashMap<>();
    String kafkaPlatform = "urn:li:dataPlatform:kafka";
    String hivePlatform = "urn:li:dataPlatform:hive";
    String bigQueryPlatform = "urn:li:dataPlatform:bigquery";

    // PROD
    platformCounts.put(kafkaPlatform, 200);
    platformCounts.put(hivePlatform, 50);
    platformCounts.put(bigQueryPlatform, 100);
    List<LineageRelationship> prodLineageRelationships =
        constructGraph(platformCounts, FabricType.PROD);

    // DEV
    platformCounts.put(kafkaPlatform, 300);
    List<LineageRelationship> devLineageRelationships =
        constructGraph(platformCounts, FabricType.DEV);

    List<LineageRelationship> lineageRelationships = new ArrayList<>();
    lineageRelationships.addAll(prodLineageRelationships);
    lineageRelationships.addAll(devLineageRelationships);

    Filter filter = QueryUtils.newFilter("platform", kafkaPlatform);
    int from = 0;
    int size = 10;
    Set<String> entityNames = Collections.emptySet();

    LineageSearchResult lineageSearchResult =
        lineageSearchService.getLightningSearchResult(
            lineageRelationships, filter, from, size, entityNames);

    assertEquals(lineageSearchResult.getNumEntities(), Integer.valueOf(500));
    assertEquals(lineageSearchResult.getEntities().size(), 10);
    assertEquals(
        lineageSearchResult.getEntities().get(0).getEntity().getEntityKey().get(0), kafkaPlatform);
    assertEquals(
        lineageSearchResult.getEntities().get(0).getEntity().getEntityKey().get(1), "name0");

    // assert that we have the right aggs per env
    assertEquals(
        lineageSearchResult.getMetadata().getAggregations().stream()
            .filter(x -> x.getName().equals("origin"))
            .map(x -> x.getAggregations().get("DEV"))
            .findFirst()
            .get(),
        Long.valueOf(300));
    assertEquals(
        lineageSearchResult.getMetadata().getAggregations().stream()
            .filter(x -> x.getName().equals("origin"))
            .map(x -> x.getAggregations().get("PROD"))
            .findFirst()
            .get(),
        Long.valueOf(200));

    // Set up filters
    ConjunctiveCriterionArray conCritArr = new ConjunctiveCriterionArray();
    Criterion platform1Crit = buildCriterion("platform", Condition.EQUAL, kafkaPlatform);

    CriterionArray critArr = new CriterionArray(ImmutableList.of(platform1Crit));
    conCritArr.add(new ConjunctiveCriterion().setAnd(critArr));
    Criterion originCrit = buildCriterion("origin", Condition.EQUAL, "DEV");

    conCritArr.add(
        new ConjunctiveCriterion().setAnd(new CriterionArray(ImmutableList.of(originCrit))));

    from = 500;
    size = 10;
    filter = new Filter().setOr(conCritArr);

    lineageSearchResult =
        lineageSearchService.getLightningSearchResult(
            lineageRelationships, filter, from, size, entityNames);

    // assert that if the query has an env filter, it is applied correctly
    assertEquals(
        lineageSearchResult.getMetadata().getAggregations().stream()
            .filter(x -> x.getName().equals("origin"))
            .map(x -> x.getAggregations().get("DEV"))
            .findFirst()
            .get(),
        Long.valueOf(300));
    assertTrue(
        lineageSearchResult.getMetadata().getAggregations().stream()
            .filter(x -> x.getName().equals("origin") && x.getAggregations().containsKey("PROD"))
            .collect(Collectors.toList())
            .isEmpty());
  }

  @Test
  public void testLightningPagination() throws Exception {
    Map<String, Integer> platformCounts = new HashMap<>();
    String kafkaPlatform = "urn:li:dataPlatform:kafka";
    String hivePlatform = "urn:li:dataPlatform:hive";
    String bigQueryPlatform = "urn:li:dataPlatform:bigquery";

    platformCounts.put(kafkaPlatform, 500);
    platformCounts.put(hivePlatform, 100);
    platformCounts.put(bigQueryPlatform, 200);

    List<LineageRelationship> lineageRelationships = constructGraph(platformCounts);

    Filter filter = QueryUtils.newFilter("platform", kafkaPlatform);
    int from = 0;
    int size = 10;
    Set<String> entityNames = Collections.emptySet();

    LineageSearchResult lineageSearchResult =
        lineageSearchService.getLightningSearchResult(
            lineageRelationships, filter, from, size, entityNames);

    assertEquals(lineageSearchResult.getNumEntities(), Integer.valueOf(500));
    assertEquals(lineageSearchResult.getEntities().size(), 10);
    assertEquals(
        lineageSearchResult.getEntities().get(0).getEntity().getEntityKey().get(0), kafkaPlatform);
    assertEquals(
        lineageSearchResult.getEntities().get(0).getEntity().getEntityKey().get(1), "name0");

    from = 50;
    size = 20;
    lineageSearchResult =
        lineageSearchService.getLightningSearchResult(
            lineageRelationships, filter, from, size, entityNames);

    assertEquals(lineageSearchResult.getNumEntities(), Integer.valueOf(500));
    assertEquals(lineageSearchResult.getEntities().size(), 20);
    assertEquals(
        lineageSearchResult.getEntities().get(0).getEntity().getEntityKey().get(0), kafkaPlatform);
    assertEquals(
        lineageSearchResult.getEntities().get(0).getEntity().getEntityKey().get(1), "name50");

    // Set up filters
    ConjunctiveCriterionArray conCritArr = new ConjunctiveCriterionArray();
    Criterion platform1Crit = buildCriterion("platform", Condition.EQUAL, kafkaPlatform);

    Criterion platform2Crit = buildCriterion("platform", Condition.EQUAL, hivePlatform);
    CriterionArray critArr = new CriterionArray(ImmutableList.of(platform1Crit));
    conCritArr.add(new ConjunctiveCriterion().setAnd(critArr));
    critArr = new CriterionArray(ImmutableList.of(platform2Crit));
    conCritArr.add(new ConjunctiveCriterion().setAnd(critArr));

    from = 500;
    size = 10;
    filter = new Filter().setOr(conCritArr);
    lineageSearchResult =
        lineageSearchService.getLightningSearchResult(
            lineageRelationships, filter, from, size, entityNames);

    assertEquals(lineageSearchResult.getNumEntities(), Integer.valueOf(600));
    assertEquals(lineageSearchResult.getEntities().size(), 10);
    assertEquals(
        lineageSearchResult.getEntities().get(0).getEntity().getEntityKey().get(0), hivePlatform);
    assertEquals(
        lineageSearchResult.getEntities().get(0).getEntity().getEntityKey().get(1), "name0");

    // Verify aggregations
    from = 0;
    size = 10;
    lineageSearchResult =
        lineageSearchService.getLightningSearchResult(
            lineageRelationships, null, from, size, entityNames);

    // Static Degree agg is the first element
    LongMap platformAggs =
        lineageSearchResult.getMetadata().getAggregations().get(1).getAggregations();
    LongMap entityTypeAggs =
        lineageSearchResult.getMetadata().getAggregations().get(2).getAggregations();
    LongMap environmentAggs =
        lineageSearchResult.getMetadata().getAggregations().get(3).getAggregations();
    assertEquals(platformAggs.get(kafkaPlatform), Long.valueOf(500));
    assertEquals(platformAggs.get(hivePlatform), Long.valueOf(100));
    assertEquals(platformAggs.get(bigQueryPlatform), Long.valueOf(200));
    assertEquals(entityTypeAggs.get(DATASET_ENTITY_NAME), Long.valueOf(800));
    assertEquals(environmentAggs.get("PROD"), Long.valueOf(800));
  }

  private List<LineageRelationship> constructGraph(Map<String, Integer> platformCounts) {
    return constructGraph(platformCounts, FabricType.PROD);
  }

  private List<LineageRelationship> constructGraph(
      Map<String, Integer> platformCounts, final FabricType env) {
    List<LineageRelationship> lineageRelationships = new ArrayList<>();
    platformCounts.forEach(
        (key, value) -> {
          for (int i = 0; i < value; i++) {
            try {
              lineageRelationships.add(
                  constructLineageRelationship(
                      new DatasetUrn(DataPlatformUrn.createFromString(key), "name" + i, env)));
            } catch (URISyntaxException e) {
              throw new RuntimeException(e);
            }
          }
        });

    return lineageRelationships;
  }

  private LineageRelationship constructLineageRelationship(Urn urn) {
    return new LineageRelationship()
        .setEntity(urn)
        .setType("DOWNSTREAM")
        .setDegree(1)
        .setPaths(new UrnArrayArray());
  }

  // Convenience method to reduce spots where we're sending the same params
  private LineageSearchResult searchAcrossLineage(@Nullable Filter filter, @Nullable String input) {
    return lineageSearchService.searchAcrossLineage(
        getOperationContext().withSearchFlags(flags -> flags.setSkipCache(true)),
        TEST_URN,
        LineageDirection.DOWNSTREAM,
        ImmutableList.of(),
        input,
        null,
        filter,
        null,
        0,
        10);
  }

  private LineageScrollResult scrollAcrossLineage(
      @Nullable Filter filter, @Nullable String input, String scrollId, int size) {
    return lineageSearchService.scrollAcrossLineage(
        getOperationContext()
            .withSearchFlags(flags -> flags.setSkipCache(true))
            .withLineageFlags(
                flags ->
                    flags
                        .setStartTimeMillis(null, SetMode.REMOVE_IF_NULL)
                        .setEndTimeMillis(null, SetMode.REMOVE_IF_NULL)),
        TEST_URN,
        LineageDirection.DOWNSTREAM,
        ImmutableList.of(),
        input,
        null,
        filter,
        null,
        scrollId,
        "5m",
        size);
  }

  private LineageScrollResult scrollAcrossLineage(@Nullable Filter filter, @Nullable String input) {
    return scrollAcrossLineage(filter, input, null, 10);
  }

  @Test
  public void testCanDoLightning() throws Exception {
    Map<String, Integer> platformCounts = new HashMap<>();
    String kafkaPlatform = "urn:li:dataPlatform:kafka";
    String hivePlatform = "urn:li:dataPlatform:hive";
    String bigQueryPlatform = "urn:li:dataPlatform:bigquery";

    platformCounts.put(kafkaPlatform, 500);
    platformCounts.put(hivePlatform, 100);
    platformCounts.put(bigQueryPlatform, 200);

    List<LineageRelationship> lineageRelationships =
        constructGraph(platformCounts, FabricType.PROD);

    Filter filter = QueryUtils.newFilter("platform", kafkaPlatform);
    int from = 0;
    int size = 10;
    Set<String> entityNames = Collections.emptySet();

    Assert.assertTrue(lineageSearchService.canDoLightning(lineageRelationships, "*", filter, null));

    // Set up filters
    ConjunctiveCriterionArray conCritArr = new ConjunctiveCriterionArray();
    Criterion platform1Crit = buildCriterion("platform", Condition.EQUAL, kafkaPlatform);

    Criterion platform2Crit = buildCriterion("platform", Condition.EQUAL, hivePlatform);

    CriterionArray critArr = new CriterionArray(ImmutableList.of(platform1Crit));
    conCritArr.add(new ConjunctiveCriterion().setAnd(critArr));
    critArr = new CriterionArray(ImmutableList.of(platform2Crit));
    conCritArr.add(new ConjunctiveCriterion().setAnd(critArr));
    Criterion originCrit = buildCriterion("origin", Condition.EQUAL, FabricType.PROD.name());
    conCritArr.add(
        new ConjunctiveCriterion().setAnd(new CriterionArray(ImmutableList.of(originCrit))));

    from = 500;
    size = 10;
    filter = new Filter().setOr(conCritArr);
    Assert.assertTrue(lineageSearchService.canDoLightning(lineageRelationships, "*", filter, null));
  }

  @Test
  public void testPlatform() {
    assertEquals(
        lineageSearchService.getPlatform(
            "dataset",
            UrnUtils.getUrn(
                "urn:li:dataset:(urn:li:dataPlatform:custom,file:///custom/path,PROD)")),
        "urn:li:dataPlatform:custom");
    assertEquals(
        lineageSearchService.getPlatform(
            "chart", UrnUtils.getUrn("urn:li:chart:(looker,foobar.1234)")),
        "urn:li:dataPlatform:looker");
    assertEquals(
        lineageSearchService.getPlatform(
            "dashboard", UrnUtils.getUrn("urn:li:dashboard:(looker,dashboards.1234)")),
        "urn:li:dataPlatform:looker");
    assertEquals(
        lineageSearchService.getPlatform(
            "dataFlow", UrnUtils.getUrn("urn:li:dataFlow:(airflow,foobar,PROD)")),
        "urn:li:dataPlatform:airflow");
    assertEquals(
        lineageSearchService.getPlatform(
            "dataJob",
            UrnUtils.getUrn("urn:li:dataJob:(urn:li:dataFlow:(airflow,foobar,PROD),End)")),
        "urn:li:dataPlatform:airflow");
  }
}
