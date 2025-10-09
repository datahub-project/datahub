package com.linkedin.metadata.search;

import static com.linkedin.metadata.utils.CriterionUtils.buildCriterion;
import static io.datahubproject.test.search.SearchTestUtils.TEST_ES_SEARCH_CONFIG;
import static io.datahubproject.test.search.SearchTestUtils.TEST_OS_SEARCH_CONFIG;
import static io.datahubproject.test.search.SearchTestUtils.TEST_OS_SEARCH_CONFIG_WITH_PIT;
import static io.datahubproject.test.search.SearchTestUtils.TEST_SEARCH_SERVICE_CONFIG;
import static io.datahubproject.test.search.SearchTestUtils.syncAfterWrite;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import com.datahub.plugins.auth.authorization.Authorizer;
import com.datahub.test.Snapshot;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.TestEntityUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.config.cache.EntityDocCountCacheConfiguration;
import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.config.search.IndexConfiguration;
import com.linkedin.metadata.config.search.SearchConfiguration;
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
import com.linkedin.metadata.search.elasticsearch.index.entity.v2.LegacyMappingsBuilder;
import com.linkedin.metadata.search.elasticsearch.index.entity.v2.LegacySettingsBuilder;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ESIndexBuilder;
import com.linkedin.metadata.search.elasticsearch.query.ESBrowseDAO;
import com.linkedin.metadata.search.elasticsearch.query.ESSearchDAO;
import com.linkedin.metadata.search.elasticsearch.query.filter.QueryFilterRewriteChain;
import com.linkedin.metadata.search.elasticsearch.update.ESBulkProcessor;
import com.linkedin.metadata.search.elasticsearch.update.ESWriteDAO;
import com.linkedin.metadata.search.ranker.SimpleRanker;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.metadata.utils.elasticsearch.IndexConventionImpl;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import com.linkedin.r2.RemoteInvocationException;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RequestContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import io.datahubproject.test.search.SearchTestUtils;
import java.net.URISyntaxException;
import java.util.Collections;
import javax.annotation.Nonnull;
import org.springframework.cache.CacheManager;
import org.springframework.cache.concurrent.ConcurrentMapCacheManager;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public abstract class SearchServiceTestBase extends AbstractTestNGSpringContextTests {

  @Nonnull
  protected abstract SearchClientShim<?> getSearchClient();

  @Nonnull
  protected abstract ESBulkProcessor getBulkProcessor();

  @Nonnull
  protected abstract ESIndexBuilder getIndexBuilder();

  @Nonnull
  protected abstract String getElasticSearchImplementation();

  @Nonnull
  protected abstract SearchConfiguration getSearchConfiguration();

  protected OperationContext operationContext;
  private LegacySettingsBuilder settingsBuilder;
  protected ElasticSearchService elasticSearchService;
  protected ElasticSearchService pitElasticSearchService;
  private CacheManager cacheManager;
  protected SearchService searchService;
  protected SearchService pitSearchService;

  protected static final String ENTITY_NAME = "testEntity";

  @BeforeClass
  public void setup() throws RemoteInvocationException, URISyntaxException {
    operationContext =
        TestOperationContexts.systemContextNoSearchAuthorization(
                new SnapshotEntityRegistry(new Snapshot()),
                new IndexConventionImpl(
                    IndexConventionImpl.IndexConventionConfig.builder()
                        .prefix("search_service_test")
                        .hashIdAlgo("MD5")
                        .build(),
                    SearchTestUtils.DEFAULT_ENTITY_INDEX_CONFIGURATION))
            .asSession(RequestContext.TEST, Authorizer.EMPTY, TestOperationContexts.TEST_USER_AUTH);

    IndexConfiguration indexConfiguration = new IndexConfiguration();
    indexConfiguration.setMinSearchFilterLength(3);
    IndexConvention indexConvention = mock(IndexConvention.class);
    when(indexConvention.isV2EntityIndex(anyString())).thenReturn(true);
    settingsBuilder = new LegacySettingsBuilder(indexConfiguration, indexConvention);
    elasticSearchService = buildEntitySearchService();
    elasticSearchService.reindexAll(operationContext, Collections.emptySet());
    pitElasticSearchService = buildPITEntitySearchService();
    pitElasticSearchService.reindexAll(operationContext, Collections.emptySet());
    cacheManager = new ConcurrentMapCacheManager();
    resetSearchService();
  }

  private void resetSearchService() {
    CachingEntitySearchService cachingEntitySearchService =
        new CachingEntitySearchService(cacheManager, elasticSearchService, 100, true);

    EntityDocCountCacheConfiguration entityDocCountCacheConfiguration =
        new EntityDocCountCacheConfiguration();
    entityDocCountCacheConfiguration.setTtlSeconds(600L);
    searchService =
        new SearchService(
            new EntityDocCountCache(
                operationContext.getEntityRegistry(),
                elasticSearchService,
                entityDocCountCacheConfiguration),
            cachingEntitySearchService,
            new SimpleRanker(),
            TEST_SEARCH_SERVICE_CONFIG);

    CachingEntitySearchService cachingPITEntitySearchService =
        new CachingEntitySearchService(cacheManager, pitElasticSearchService, 100, true);
    pitSearchService =
        new SearchService(
            new EntityDocCountCache(
                operationContext.getEntityRegistry(),
                pitElasticSearchService,
                entityDocCountCacheConfiguration),
            cachingPITEntitySearchService,
            new SimpleRanker(),
            TEST_SEARCH_SERVICE_CONFIG);
  }

  @BeforeMethod
  public void wipe() throws Exception {
    syncAfterWrite(getBulkProcessor());
    elasticSearchService.clear(operationContext);
    syncAfterWrite(getBulkProcessor());
  }

  @Nonnull
  private ElasticSearchService buildEntitySearchService() {
    ElasticSearchConfiguration esConfig = TEST_OS_SEARCH_CONFIG.toBuilder().build();
    ESSearchDAO searchDAO =
        new ESSearchDAO(
            getSearchClient(),
            esConfig.getSearch().isPointInTimeCreationEnabled(),
            esConfig,
            null,
            QueryFilterRewriteChain.EMPTY,
            TEST_SEARCH_SERVICE_CONFIG);
    ESBrowseDAO browseDAO =
        new ESBrowseDAO(
            getSearchClient(),
            esConfig,
            null,
            QueryFilterRewriteChain.EMPTY,
            TEST_SEARCH_SERVICE_CONFIG);
    ESWriteDAO writeDAO = new ESWriteDAO(esConfig, getSearchClient(), getBulkProcessor());
    ElasticSearchService searchService =
        new ElasticSearchService(
            getIndexBuilder(),
            TEST_SEARCH_SERVICE_CONFIG,
            TEST_ES_SEARCH_CONFIG,
            new LegacyMappingsBuilder(TEST_ES_SEARCH_CONFIG.getEntityIndex()),
            searchDAO,
            browseDAO,
            writeDAO);
    return searchService;
  }

  @Nonnull
  private ElasticSearchService buildPITEntitySearchService() {
    ElasticSearchConfiguration esConfig = TEST_OS_SEARCH_CONFIG_WITH_PIT.toBuilder().build();
    ESSearchDAO searchDAO =
        new ESSearchDAO(
            getSearchClient(),
            esConfig.getSearch().isPointInTimeCreationEnabled(),
            esConfig,
            null,
            QueryFilterRewriteChain.EMPTY,
            TEST_SEARCH_SERVICE_CONFIG);
    ESBrowseDAO browseDAO =
        new ESBrowseDAO(
            getSearchClient(),
            esConfig,
            null,
            QueryFilterRewriteChain.EMPTY,
            TEST_SEARCH_SERVICE_CONFIG);
    ESWriteDAO writeDAO = new ESWriteDAO(esConfig, getSearchClient(), getBulkProcessor());
    ElasticSearchService searchService =
        new ElasticSearchService(
            getIndexBuilder(),
            TEST_SEARCH_SERVICE_CONFIG,
            esConfig,
            new LegacyMappingsBuilder(esConfig.getEntityIndex()),
            searchDAO,
            browseDAO,
            writeDAO);
    return searchService;
  }

  protected void clearCache() {
    cacheManager.getCacheNames().forEach(cache -> cacheManager.getCache(cache).clear());
    resetSearchService();
  }

  @Test
  public void testSearchService() throws Exception {
    SearchResult searchResult =
        searchService.searchAcrossEntities(
            operationContext.withSearchFlags(flags -> flags.setFulltext(true).setSkipCache(true)),
            ImmutableList.of(ENTITY_NAME),
            "test",
            null,
            null,
            0,
            10);
    assertEquals(searchResult.getNumEntities().intValue(), 0);
    searchResult =
        searchService.searchAcrossEntities(
            operationContext.withSearchFlags(flags -> flags.setFulltext(true)),
            ImmutableList.of(),
            "test",
            null,
            null,
            0,
            10);
    assertEquals(searchResult.getNumEntities().intValue(), 0);
    clearCache();

    Urn urn = new TestEntityUrn("test", "urn1", "VALUE_1");
    ObjectNode document = JsonNodeFactory.instance.objectNode();
    document.set("urn", JsonNodeFactory.instance.textNode(urn.toString()));
    document.set("keyPart1", JsonNodeFactory.instance.textNode("test"));
    document.set("textFieldOverride", JsonNodeFactory.instance.textNode("textFieldOverride"));
    document.set("browsePaths", JsonNodeFactory.instance.textNode("/a/b/c"));
    elasticSearchService.upsertDocument(
        operationContext, ENTITY_NAME, document.toString(), urn.toString());
    syncAfterWrite(getBulkProcessor());

    searchResult =
        searchService.searchAcrossEntities(
            operationContext.withSearchFlags(flags -> flags.setFulltext(true)),
            ImmutableList.of(),
            "test",
            null,
            null,
            0,
            10);
    assertEquals(searchResult.getNumEntities().intValue(), 1);
    assertEquals(searchResult.getEntities().get(0).getEntity(), urn);
    clearCache();

    Urn urn2 = new TestEntityUrn("test2", "urn2", "VALUE_2");
    ObjectNode document2 = JsonNodeFactory.instance.objectNode();
    document2.set("urn", JsonNodeFactory.instance.textNode(urn2.toString()));
    document2.set("keyPart1", JsonNodeFactory.instance.textNode("random"));
    document2.set("textFieldOverride", JsonNodeFactory.instance.textNode("textFieldOverride2"));
    document2.set("browsePaths", JsonNodeFactory.instance.textNode("/b/c"));
    elasticSearchService.upsertDocument(
        operationContext, ENTITY_NAME, document2.toString(), urn2.toString());
    syncAfterWrite(getBulkProcessor());

    searchResult =
        searchService.searchAcrossEntities(
            operationContext.withSearchFlags(flags -> flags.setFulltext(true)),
            ImmutableList.of(),
            "'test2'",
            null,
            null,
            0,
            10);
    assertEquals(searchResult.getNumEntities().intValue(), 1);
    assertEquals(searchResult.getEntities().get(0).getEntity(), urn2);
    clearCache();

    long docCount =
        elasticSearchService.docCount(
            operationContext.withSearchFlags(flags -> flags.setFulltext(false)), ENTITY_NAME);
    assertEquals(docCount, 2L);

    elasticSearchService.deleteDocument(operationContext, ENTITY_NAME, urn.toString());
    elasticSearchService.deleteDocument(operationContext, ENTITY_NAME, urn2.toString());
    syncAfterWrite(getBulkProcessor());
    searchResult =
        searchService.searchAcrossEntities(
            operationContext.withSearchFlags(flags -> flags.setFulltext(true)),
            ImmutableList.of(),
            "'test2'",
            null,
            null,
            0,
            10);
    assertEquals(searchResult.getNumEntities().intValue(), 0);
  }

  @Test
  public void testAdvancedSearchOr() throws Exception {
    final Criterion filterCriterion = buildCriterion("platform", Condition.EQUAL, "hive");

    final Criterion subtypeCriterion = buildCriterion("subtypes", Condition.EQUAL, "view");

    final Filter filterWithCondition =
        new Filter()
            .setOr(
                new ConjunctiveCriterionArray(
                    new ConjunctiveCriterion()
                        .setAnd(new CriterionArray(ImmutableList.of(filterCriterion))),
                    new ConjunctiveCriterion()
                        .setAnd(new CriterionArray(ImmutableList.of(subtypeCriterion)))));

    SearchResult searchResult =
        searchService.searchAcrossEntities(
            operationContext.withSearchFlags(flags -> flags.setFulltext(true)),
            ImmutableList.of(ENTITY_NAME),
            "test",
            filterWithCondition,
            null,
            0,
            10);

    assertEquals(searchResult.getNumEntities().intValue(), 0);
    clearCache();

    Urn urn = new TestEntityUrn("test", "testUrn", "VALUE_1");
    ObjectNode document = JsonNodeFactory.instance.objectNode();
    document.set("urn", JsonNodeFactory.instance.textNode(urn.toString()));
    document.set("keyPart1", JsonNodeFactory.instance.textNode("test"));
    document.set("textFieldOverride", JsonNodeFactory.instance.textNode("textFieldOverride"));
    document.set("browsePaths", JsonNodeFactory.instance.textNode("/a/b/c"));
    document.set("subtypes", JsonNodeFactory.instance.textNode("view"));
    document.set("platform", JsonNodeFactory.instance.textNode("snowflake"));
    elasticSearchService.upsertDocument(
        operationContext, ENTITY_NAME, document.toString(), urn.toString());

    Urn urn2 = new TestEntityUrn("test", "testUrn", "VALUE_2");
    ObjectNode document2 = JsonNodeFactory.instance.objectNode();
    document2.set("urn", JsonNodeFactory.instance.textNode(urn2.toString()));
    document2.set("keyPart1", JsonNodeFactory.instance.textNode("test"));
    document2.set("textFieldOverride", JsonNodeFactory.instance.textNode("textFieldOverride"));
    document2.set("browsePaths", JsonNodeFactory.instance.textNode("/a/b/c"));
    document2.set("subtypes", JsonNodeFactory.instance.textNode("table"));
    document2.set("platform", JsonNodeFactory.instance.textNode("hive"));
    elasticSearchService.upsertDocument(
        operationContext, ENTITY_NAME, document2.toString(), urn2.toString());

    Urn urn3 = new TestEntityUrn("test", "testUrn", "VALUE_3");
    ObjectNode document3 = JsonNodeFactory.instance.objectNode();
    document3.set("urn", JsonNodeFactory.instance.textNode(urn3.toString()));
    document3.set("keyPart1", JsonNodeFactory.instance.textNode("test"));
    document3.set("textFieldOverride", JsonNodeFactory.instance.textNode("textFieldOverride"));
    document3.set("browsePaths", JsonNodeFactory.instance.textNode("/a/b/c"));
    document3.set("subtypes", JsonNodeFactory.instance.textNode("table"));
    document3.set("platform", JsonNodeFactory.instance.textNode("snowflake"));
    elasticSearchService.upsertDocument(
        operationContext, ENTITY_NAME, document3.toString(), urn3.toString());

    syncAfterWrite(getBulkProcessor());

    searchResult =
        searchService.searchAcrossEntities(
            operationContext.withSearchFlags(flags -> flags.setFulltext(true)),
            ImmutableList.of(),
            "test",
            filterWithCondition,
            null,
            0,
            10);
    assertEquals(searchResult.getNumEntities().intValue(), 2);
    assertEquals(searchResult.getEntities().get(0).getEntity(), urn);
    assertEquals(searchResult.getEntities().get(1).getEntity(), urn2);
    clearCache();
  }

  @Test
  public void testAdvancedSearchSoftDelete() throws Exception {
    final Criterion filterCriterion = buildCriterion("platform", Condition.EQUAL, "hive");

    final Criterion removedCriterion = buildCriterion("removed", Condition.EQUAL, "true");

    final Filter filterWithCondition =
        new Filter()
            .setOr(
                new ConjunctiveCriterionArray(
                    new ConjunctiveCriterion()
                        .setAnd(
                            new CriterionArray(
                                ImmutableList.of(filterCriterion, removedCriterion)))));

    SearchResult searchResult =
        searchService.searchAcrossEntities(
            operationContext.withSearchFlags(flags -> flags.setFulltext(true)),
            ImmutableList.of(ENTITY_NAME),
            "test",
            filterWithCondition,
            null,
            0,
            10);

    assertEquals(searchResult.getNumEntities().intValue(), 0);
    clearCache();

    Urn urn = new TestEntityUrn("test", "testUrn", "VALUE_1");
    ObjectNode document = JsonNodeFactory.instance.objectNode();
    document.set("urn", JsonNodeFactory.instance.textNode(urn.toString()));
    document.set("keyPart1", JsonNodeFactory.instance.textNode("test"));
    document.set("textFieldOverride", JsonNodeFactory.instance.textNode("textFieldOverride"));
    document.set("browsePaths", JsonNodeFactory.instance.textNode("/a/b/c"));
    document.set("subtypes", JsonNodeFactory.instance.textNode("view"));
    document.set("platform", JsonNodeFactory.instance.textNode("hive"));
    document.set("removed", JsonNodeFactory.instance.booleanNode(true));
    elasticSearchService.upsertDocument(
        operationContext, ENTITY_NAME, document.toString(), urn.toString());

    Urn urn2 = new TestEntityUrn("test", "testUrn", "VALUE_2");
    ObjectNode document2 = JsonNodeFactory.instance.objectNode();
    document2.set("urn", JsonNodeFactory.instance.textNode(urn2.toString()));
    document2.set("keyPart1", JsonNodeFactory.instance.textNode("test"));
    document2.set("textFieldOverride", JsonNodeFactory.instance.textNode("textFieldOverride"));
    document2.set("browsePaths", JsonNodeFactory.instance.textNode("/a/b/c"));
    document2.set("subtypes", JsonNodeFactory.instance.textNode("table"));
    document2.set("platform", JsonNodeFactory.instance.textNode("hive"));
    document.set("removed", JsonNodeFactory.instance.booleanNode(false));
    elasticSearchService.upsertDocument(
        operationContext, ENTITY_NAME, document2.toString(), urn2.toString());

    Urn urn3 = new TestEntityUrn("test", "testUrn", "VALUE_3");
    ObjectNode document3 = JsonNodeFactory.instance.objectNode();
    document3.set("urn", JsonNodeFactory.instance.textNode(urn3.toString()));
    document3.set("keyPart1", JsonNodeFactory.instance.textNode("test"));
    document3.set("textFieldOverride", JsonNodeFactory.instance.textNode("textFieldOverride"));
    document3.set("browsePaths", JsonNodeFactory.instance.textNode("/a/b/c"));
    document3.set("subtypes", JsonNodeFactory.instance.textNode("table"));
    document3.set("platform", JsonNodeFactory.instance.textNode("snowflake"));
    document.set("removed", JsonNodeFactory.instance.booleanNode(false));
    elasticSearchService.upsertDocument(
        operationContext, ENTITY_NAME, document3.toString(), urn3.toString());

    syncAfterWrite(getBulkProcessor());

    searchResult =
        searchService.searchAcrossEntities(
            operationContext.withSearchFlags(flags -> flags.setFulltext(true)),
            ImmutableList.of(),
            "test",
            filterWithCondition,
            null,
            0,
            10);
    assertEquals(searchResult.getNumEntities().intValue(), 1);
    assertEquals(searchResult.getEntities().get(0).getEntity(), urn);
    clearCache();
  }

  @Test
  public void testAdvancedSearchNegated() throws Exception {
    final Criterion filterCriterion = buildCriterion("platform", Condition.EQUAL, true, "hive");

    final Filter filterWithCondition =
        new Filter()
            .setOr(
                new ConjunctiveCriterionArray(
                    new ConjunctiveCriterion()
                        .setAnd(new CriterionArray(ImmutableList.of(filterCriterion)))));

    SearchResult searchResult =
        searchService.searchAcrossEntities(
            operationContext.withSearchFlags(flags -> flags.setFulltext(true)),
            ImmutableList.of(ENTITY_NAME),
            "test",
            filterWithCondition,
            null,
            0,
            10);

    assertEquals(searchResult.getNumEntities().intValue(), 0);
    clearCache();

    Urn urn = new TestEntityUrn("test", "testUrn", "VALUE_1");
    ObjectNode document = JsonNodeFactory.instance.objectNode();
    document.set("urn", JsonNodeFactory.instance.textNode(urn.toString()));
    document.set("keyPart1", JsonNodeFactory.instance.textNode("test"));
    document.set("textFieldOverride", JsonNodeFactory.instance.textNode("textFieldOverride"));
    document.set("browsePaths", JsonNodeFactory.instance.textNode("/a/b/c"));
    document.set("subtypes", JsonNodeFactory.instance.textNode("view"));
    document.set("platform", JsonNodeFactory.instance.textNode("hive"));
    document.set("removed", JsonNodeFactory.instance.booleanNode(true));
    elasticSearchService.upsertDocument(
        operationContext, ENTITY_NAME, document.toString(), urn.toString());

    Urn urn2 = new TestEntityUrn("test", "testUrn", "VALUE_2");
    ObjectNode document2 = JsonNodeFactory.instance.objectNode();
    document2.set("urn", JsonNodeFactory.instance.textNode(urn2.toString()));
    document2.set("keyPart1", JsonNodeFactory.instance.textNode("test"));
    document2.set("textFieldOverride", JsonNodeFactory.instance.textNode("textFieldOverride"));
    document2.set("browsePaths", JsonNodeFactory.instance.textNode("/a/b/c"));
    document2.set("subtypes", JsonNodeFactory.instance.textNode("table"));
    document2.set("platform", JsonNodeFactory.instance.textNode("hive"));
    document.set("removed", JsonNodeFactory.instance.booleanNode(false));
    elasticSearchService.upsertDocument(
        operationContext, ENTITY_NAME, document2.toString(), urn2.toString());

    Urn urn3 = new TestEntityUrn("test", "testUrn", "VALUE_3");
    ObjectNode document3 = JsonNodeFactory.instance.objectNode();
    document3.set("urn", JsonNodeFactory.instance.textNode(urn3.toString()));
    document3.set("keyPart1", JsonNodeFactory.instance.textNode("test"));
    document3.set("textFieldOverride", JsonNodeFactory.instance.textNode("textFieldOverride"));
    document3.set("browsePaths", JsonNodeFactory.instance.textNode("/a/b/c"));
    document3.set("subtypes", JsonNodeFactory.instance.textNode("table"));
    document3.set("platform", JsonNodeFactory.instance.textNode("snowflake"));
    document.set("removed", JsonNodeFactory.instance.booleanNode(false));
    elasticSearchService.upsertDocument(
        operationContext, ENTITY_NAME, document3.toString(), urn3.toString());

    syncAfterWrite(getBulkProcessor());

    searchResult =
        searchService.searchAcrossEntities(
            operationContext.withSearchFlags(flags -> flags.setFulltext(true)),
            ImmutableList.of(),
            "test",
            filterWithCondition,
            null,
            0,
            10);
    assertEquals(searchResult.getNumEntities().intValue(), 1);
    assertEquals(searchResult.getEntities().get(0).getEntity(), urn3);
    clearCache();
  }

  @Test
  public void testSearchWithSearchAfter() throws Exception {
    // Set up test data
    Urn urn1 = new TestEntityUrn("pit", "testUrn1", "VALUE_1");
    ObjectNode document1 = JsonNodeFactory.instance.objectNode();
    document1.set("urn", JsonNodeFactory.instance.textNode(urn1.toString()));
    document1.set("keyPart1", JsonNodeFactory.instance.textNode("pit_test_data"));
    document1.set("textFieldOverride", JsonNodeFactory.instance.textNode("pit_test_field1"));
    document1.set("browsePaths", JsonNodeFactory.instance.textNode("/pit/test/1"));
    elasticSearchService.upsertDocument(
        operationContext, ENTITY_NAME, document1.toString(), urn1.toString());

    Urn urn2 = new TestEntityUrn("pit", "testUrn2", "VALUE_2");
    ObjectNode document2 = JsonNodeFactory.instance.objectNode();
    document2.set("urn", JsonNodeFactory.instance.textNode(urn2.toString()));
    document2.set("keyPart1", JsonNodeFactory.instance.textNode("pit_test_data"));
    document2.set("textFieldOverride", JsonNodeFactory.instance.textNode("pit_test_field2"));
    document2.set("browsePaths", JsonNodeFactory.instance.textNode("/pit/test/2"));
    elasticSearchService.upsertDocument(
        operationContext, ENTITY_NAME, document2.toString(), urn2.toString());

    Urn urn3 = new TestEntityUrn("pit", "testUrn3", "VALUE_3");
    ObjectNode document3 = JsonNodeFactory.instance.objectNode();
    document3.set("urn", JsonNodeFactory.instance.textNode(urn3.toString()));
    document3.set("keyPart1", JsonNodeFactory.instance.textNode("pit_test_data"));
    document3.set("textFieldOverride", JsonNodeFactory.instance.textNode("pit_test_field3"));
    document3.set("browsePaths", JsonNodeFactory.instance.textNode("/pit/test/3"));
    elasticSearchService.upsertDocument(
        operationContext, ENTITY_NAME, document3.toString(), urn3.toString());

    Urn urn4 = new TestEntityUrn("pit", "testUrn4", "VALUE_4");
    ObjectNode document4 = JsonNodeFactory.instance.objectNode();
    document4.set("urn", JsonNodeFactory.instance.textNode(urn4.toString()));
    document4.set("keyPart1", JsonNodeFactory.instance.textNode("pit_test_data"));
    document4.set("textFieldOverride", JsonNodeFactory.instance.textNode("pit_test_field4"));
    document4.set("browsePaths", JsonNodeFactory.instance.textNode("/pit/test/4"));
    elasticSearchService.upsertDocument(
        operationContext, ENTITY_NAME, document4.toString(), urn4.toString());

    syncAfterWrite(getBulkProcessor());
    clearCache();

    ScrollResult searchResultAll =
        searchService.scrollAcrossEntities(
            operationContext.withSearchFlags(flags -> flags.setFulltext(true).setSkipCache(true)),
            ImmutableList.of(),
            "pit_test_data",
            null,
            null,
            null,
            "2m",
            3,
            null);
    assertEquals(searchResultAll.getEntities().size(), 3);
    searchResultAll =
        searchService.scrollAcrossEntities(
            operationContext.withSearchFlags(flags -> flags.setFulltext(true).setSkipCache(true)),
            ImmutableList.of(),
            "pit_test_data",
            null,
            null,
            searchResultAll.getScrollId(),
            "2m",
            3,
            null);
    assertEquals(searchResultAll.getEntities().size(), 1);

    // Clean up test data
    elasticSearchService.deleteDocument(operationContext, ENTITY_NAME, urn1.toString());
    elasticSearchService.deleteDocument(operationContext, ENTITY_NAME, urn2.toString());
    elasticSearchService.deleteDocument(operationContext, ENTITY_NAME, urn3.toString());
    elasticSearchService.deleteDocument(operationContext, ENTITY_NAME, urn4.toString());
    syncAfterWrite(getBulkProcessor());

    // Verify cleanup
    ScrollResult searchResultAfterCleanup =
        searchService.scrollAcrossEntities(
            operationContext.withSearchFlags(flags -> flags.setFulltext(true).setSkipCache(true)),
            ImmutableList.of(),
            "pit_test_data",
            null,
            null,
            null,
            "2m",
            10,
            null);
    assertEquals(searchResultAfterCleanup.getNumEntities().intValue(), 0);

    clearCache();
  }

  @Test
  public void testSearchWithPIT() throws Exception {
    // Set up test data
    Urn urn1 = new TestEntityUrn("pit", "testUrn1", "VALUE_1");
    ObjectNode document1 = JsonNodeFactory.instance.objectNode();
    document1.set("urn", JsonNodeFactory.instance.textNode(urn1.toString()));
    document1.set("keyPart1", JsonNodeFactory.instance.textNode("pit_test_data"));
    document1.set("textFieldOverride", JsonNodeFactory.instance.textNode("pit_test_field1"));
    document1.set("browsePaths", JsonNodeFactory.instance.textNode("/pit/test/1"));
    pitElasticSearchService.upsertDocument(
        operationContext, ENTITY_NAME, document1.toString(), urn1.toString());

    Urn urn2 = new TestEntityUrn("pit", "testUrn2", "VALUE_2");
    ObjectNode document2 = JsonNodeFactory.instance.objectNode();
    document2.set("urn", JsonNodeFactory.instance.textNode(urn2.toString()));
    document2.set("keyPart1", JsonNodeFactory.instance.textNode("pit_test_data"));
    document2.set("textFieldOverride", JsonNodeFactory.instance.textNode("pit_test_field2"));
    document2.set("browsePaths", JsonNodeFactory.instance.textNode("/pit/test/2"));
    pitElasticSearchService.upsertDocument(
        operationContext, ENTITY_NAME, document2.toString(), urn2.toString());

    Urn urn3 = new TestEntityUrn("pit", "testUrn3", "VALUE_3");
    ObjectNode document3 = JsonNodeFactory.instance.objectNode();
    document3.set("urn", JsonNodeFactory.instance.textNode(urn3.toString()));
    document3.set("keyPart1", JsonNodeFactory.instance.textNode("pit_test_data"));
    document3.set("textFieldOverride", JsonNodeFactory.instance.textNode("pit_test_field3"));
    document3.set("browsePaths", JsonNodeFactory.instance.textNode("/pit/test/3"));
    pitElasticSearchService.upsertDocument(
        operationContext, ENTITY_NAME, document3.toString(), urn3.toString());

    Urn urn4 = new TestEntityUrn("pit", "testUrn4", "VALUE_4");
    ObjectNode document4 = JsonNodeFactory.instance.objectNode();
    document4.set("urn", JsonNodeFactory.instance.textNode(urn4.toString()));
    document4.set("keyPart1", JsonNodeFactory.instance.textNode("pit_test_data"));
    document4.set("textFieldOverride", JsonNodeFactory.instance.textNode("pit_test_field4"));
    document4.set("browsePaths", JsonNodeFactory.instance.textNode("/pit/test/4"));
    pitElasticSearchService.upsertDocument(
        operationContext, ENTITY_NAME, document4.toString(), urn4.toString());

    syncAfterWrite(getBulkProcessor());
    clearCache();

    ScrollResult searchResultAll =
        pitSearchService.scrollAcrossEntities(
            operationContext.withSearchFlags(flags -> flags.setFulltext(true).setSkipCache(true)),
            ImmutableList.of(),
            "pit_test_data",
            null,
            null,
            null,
            "2m",
            3,
            null);
    assertEquals(searchResultAll.getEntities().size(), 3);
    searchResultAll =
        pitSearchService.scrollAcrossEntities(
            operationContext.withSearchFlags(flags -> flags.setFulltext(true).setSkipCache(true)),
            ImmutableList.of(),
            "pit_test_data",
            null,
            null,
            searchResultAll.getScrollId(),
            "2m",
            3,
            null);
    assertEquals(searchResultAll.getEntities().size(), 1);

    // Clean up test data
    pitElasticSearchService.deleteDocument(operationContext, ENTITY_NAME, urn1.toString());
    pitElasticSearchService.deleteDocument(operationContext, ENTITY_NAME, urn2.toString());
    pitElasticSearchService.deleteDocument(operationContext, ENTITY_NAME, urn3.toString());
    pitElasticSearchService.deleteDocument(operationContext, ENTITY_NAME, urn4.toString());
    syncAfterWrite(getBulkProcessor());

    // Verify cleanup
    ScrollResult searchResultAfterCleanup =
        pitSearchService.scrollAcrossEntities(
            operationContext.withSearchFlags(flags -> flags.setFulltext(true).setSkipCache(true)),
            ImmutableList.of(),
            "pit_test_data",
            null,
            null,
            null,
            "2m",
            10,
            null);
    assertEquals(searchResultAfterCleanup.getNumEntities().intValue(), 0);

    clearCache();
  }
}
