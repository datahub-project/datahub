package com.linkedin.metadata.search;

import static com.linkedin.metadata.Constants.ELASTICSEARCH_IMPLEMENTATION_ELASTICSEARCH;
import static com.linkedin.metadata.utils.CriterionUtils.buildCriterion;
import static io.datahubproject.test.search.SearchTestUtils.syncAfterWrite;
import static org.testng.Assert.assertEquals;

import com.datahub.plugins.auth.authorization.Authorizer;
import com.datahub.test.Snapshot;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.TestEntityUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.config.cache.EntityDocCountCacheConfiguration;
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
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ESIndexBuilder;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.EntityIndexBuilders;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.SettingsBuilder;
import com.linkedin.metadata.search.elasticsearch.query.ESBrowseDAO;
import com.linkedin.metadata.search.elasticsearch.query.ESSearchDAO;
import com.linkedin.metadata.search.elasticsearch.query.filter.QueryFilterRewriteChain;
import com.linkedin.metadata.search.elasticsearch.update.ESBulkProcessor;
import com.linkedin.metadata.search.elasticsearch.update.ESWriteDAO;
import com.linkedin.metadata.search.ranker.SimpleRanker;
import com.linkedin.metadata.utils.elasticsearch.IndexConventionImpl;
import com.linkedin.r2.RemoteInvocationException;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RequestContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.net.URISyntaxException;
import java.util.Collections;
import javax.annotation.Nonnull;
import org.opensearch.client.RestHighLevelClient;
import org.springframework.cache.CacheManager;
import org.springframework.cache.concurrent.ConcurrentMapCacheManager;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public abstract class SearchServiceTestBase extends AbstractTestNGSpringContextTests {

  @Nonnull
  protected abstract RestHighLevelClient getSearchClient();

  @Nonnull
  protected abstract ESBulkProcessor getBulkProcessor();

  @Nonnull
  protected abstract ESIndexBuilder getIndexBuilder();

  @Nonnull
  protected abstract SearchConfiguration getSearchConfiguration();

  protected OperationContext operationContext;
  private SettingsBuilder settingsBuilder;
  private ElasticSearchService elasticSearchService;
  private CacheManager cacheManager;
  private SearchService searchService;

  private static final String ENTITY_NAME = "testEntity";

  @BeforeClass
  public void setup() throws RemoteInvocationException, URISyntaxException {
    operationContext =
        TestOperationContexts.systemContextNoSearchAuthorization(
                new SnapshotEntityRegistry(new Snapshot()),
                new IndexConventionImpl(
                    IndexConventionImpl.IndexConventionConfig.builder()
                        .prefix("search_service_test")
                        .hashIdAlgo("MD5")
                        .build()))
            .asSession(RequestContext.TEST, Authorizer.EMPTY, TestOperationContexts.TEST_USER_AUTH);

    settingsBuilder = new SettingsBuilder(null);
    elasticSearchService = buildEntitySearchService();
    elasticSearchService.reindexAll(Collections.emptySet());
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
            new SimpleRanker());
  }

  @BeforeMethod
  public void wipe() throws Exception {
    syncAfterWrite(getBulkProcessor());
    elasticSearchService.clear(operationContext);
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
    ESSearchDAO searchDAO =
        new ESSearchDAO(
            getSearchClient(),
            false,
            ELASTICSEARCH_IMPLEMENTATION_ELASTICSEARCH,
            getSearchConfiguration(),
            null,
            QueryFilterRewriteChain.EMPTY);
    ESBrowseDAO browseDAO =
        new ESBrowseDAO(
            getSearchClient(), getSearchConfiguration(), null, QueryFilterRewriteChain.EMPTY);
    ESWriteDAO writeDAO = new ESWriteDAO(getSearchClient(), getBulkProcessor(), 1);
    return new ElasticSearchService(indexBuilders, searchDAO, browseDAO, writeDAO);
  }

  private void clearCache() {
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
}
