package com.linkedin.metadata.search.elasticsearch.query;

import static io.datahubproject.test.search.SearchTestUtils.TEST_OS_SEARCH_CONFIG;
import static io.datahubproject.test.search.SearchTestUtils.TEST_SEARCH_SERVICE_CONFIG;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.config.search.EntityIndexConfiguration;
import com.linkedin.metadata.config.search.EntityIndexVersionConfiguration;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.query.filter.SortOrder;
import com.linkedin.metadata.search.elasticsearch.query.filter.QueryFilterRewriteChain;
import com.linkedin.metadata.search.elasticsearch.query.request.SearchQueryBuilder;
import com.linkedin.metadata.search.utils.QueryUtils;
import com.linkedin.metadata.utils.CriterionUtils;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.List;
import org.apache.lucene.search.TotalHits;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.ShardSearchFailure;
import org.opensearch.client.RequestOptions;
import org.opensearch.common.lucene.search.function.CombineFunction;
import org.opensearch.common.lucene.search.function.FunctionScoreQuery;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.opensearch.index.query.functionscore.ScoreFunctionBuilders;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ESSearchDAOLightFirstTest {

  private static final QueryBuilder FULL = QueryBuilders.matchQuery("name", "full");
  private static final QueryBuilder LIGHT = QueryBuilders.matchQuery("name", "light");
  private static final QueryBuilder FILTER = QueryBuilders.termQuery("platform", "hive");

  private SearchClientShim<?> client;
  private OperationContext opContext;
  private ESSearchDAO dao;

  @BeforeMethod
  public void setup() {
    client = mock(SearchClientShim.class);
    opContext =
        TestOperationContexts.withFixedSearchClient(
            TestOperationContexts.systemContextNoValidate(), client);
    dao =
        new ESSearchDAO(
            false,
            TEST_OS_SEARCH_CONFIG,
            null,
            QueryFilterRewriteChain.EMPTY,
            TEST_SEARCH_SERVICE_CONFIG);
  }

  @Test
  public void testLightSourceQueryKeepsFilters() {
    BoolQueryBuilder light =
        (BoolQueryBuilder)
            ESSearchDAO.buildLightSourceQuery(
                QueryBuilders.boolQuery().must(FULL).filter(FILTER).mustNot(FILTER), LIGHT);
    assertEquals(light.must().size(), 1);
    assertSame(light.must().get(0), LIGHT);
    assertEquals(light.filter().size(), 1);
    assertSame(light.filter().get(0), FILTER);
    assertSame(light.mustNot().get(0), FILTER);
  }

  @Test
  public void testLightSourceQueryKeepsFunctionScore() {
    FunctionScoreQueryBuilder original =
        QueryBuilders.functionScoreQuery(
                QueryBuilders.boolQuery().must(FULL).filter(FILTER),
                new FunctionScoreQueryBuilder.FilterFunctionBuilder[] {
                  new FunctionScoreQueryBuilder.FilterFunctionBuilder(
                      ScoreFunctionBuilders.weightFactorFunction(2.0f))
                })
            .scoreMode(FunctionScoreQuery.ScoreMode.MULTIPLY)
            .boostMode(CombineFunction.REPLACE);
    original.setMinScore(0.5f);

    FunctionScoreQueryBuilder light =
        (FunctionScoreQueryBuilder) ESSearchDAO.buildLightSourceQuery(original, LIGHT);
    assertSame(((BoolQueryBuilder) light.query()).must().get(0), LIGHT);
    assertSame(((BoolQueryBuilder) light.query()).filter().get(0), FILTER);
    assertEquals(light.filterFunctionBuilders(), original.filterFunctionBuilders());
    assertEquals(light.scoreMode(), FunctionScoreQuery.ScoreMode.MULTIPLY);
    assertEquals(light.boostMode(), CombineFunction.REPLACE);
    assertEquals(light.getMinScore(), 0.5f);
  }

  @Test
  public void testLightSourceQueryFailsClosed() {
    // The caller runs the full query
    assertNull(ESSearchDAO.buildLightSourceQuery(FULL, LIGHT));
    // A root clause the light query would drop
    assertNull(
        ESSearchDAO.buildLightSourceQuery(
            QueryBuilders.boolQuery().must(FULL).should(FILTER), LIGHT));
    assertNull(
        ESSearchDAO.buildLightSourceQuery(
            QueryBuilders.boolQuery().must(FULL).must(FILTER), LIGHT));
  }

  @Test
  public void testLightResultWithFailedShardsNeverStops() throws Exception {
    SearchRequest request = request(QueryBuilders.boolQuery().must(FULL));
    // An ID lookup the light query does not find stops there
    SearchResponse cleanEmpty = response(0, 0);
    when(client.search(any(OperationContext.class), any(), eq(RequestOptions.DEFAULT)))
        .thenReturn(cleanEmpty);
    assertSame(dao.searchLightFirst(opContext, request, LIGHT, "run_20240101"), cleanEmpty);
    // Unless a shard failed, which can hide the matches: the full query runs
    SearchResponse failedEmpty = response(0, 1);
    SearchResponse full = response(3, 0);
    when(client.search(any(OperationContext.class), any(), eq(RequestOptions.DEFAULT)))
        .thenReturn(failedEmpty, full);
    assertSame(dao.searchLightFirst(opContext, request, LIGHT, "run_20240101"), full);
    // So does a light query that timed out
    SearchResponse timedOutEmpty = response(0, 0);
    when(timedOutEmpty.isTimedOut()).thenReturn(true);
    when(client.search(any(OperationContext.class), any(), eq(RequestOptions.DEFAULT)))
        .thenReturn(timedOutEmpty, full);
    assertSame(dao.searchLightFirst(opContext, request, LIGHT, "run_20240101"), full);
    // Hits are served even when a shard failed
    SearchResponse failedHits = response(2, 1);
    when(client.search(any(OperationContext.class), any(), eq(RequestOptions.DEFAULT)))
        .thenReturn(failedHits);
    assertSame(dao.searchLightFirst(opContext, request, LIGHT, "orders"), failedHits);
    // One query for the stop and for the served hits, two for each fall-through
    verify(client, times(6)).search(any(OperationContext.class), any(), eq(RequestOptions.DEFAULT));
    // The request keeps the full query for the caller
    assertSame(((BoolQueryBuilder) request.source().query()).must().get(0), FULL);
  }

  @Test
  public void testRootQueryTheLightQueryCannotCarryRunsOnlyTheFullQuery() throws Exception {
    SearchRequest request = request(FULL);
    SearchResponse full = response(3, 0);
    when(client.search(any(OperationContext.class), any(), eq(RequestOptions.DEFAULT)))
        .thenReturn(full);
    assertSame(dao.searchLightFirst(opContext, request, LIGHT, "orders"), full);
    verify(client).search(any(OperationContext.class), any(), eq(RequestOptions.DEFAULT));
    assertSame(request.source().query(), FULL);
  }

  @Test
  public void testLightQueryGates() {
    ESSearchDAO v3Dao =
        new ESSearchDAO(
            false,
            TEST_OS_SEARCH_CONFIG.toBuilder()
                .entityIndex(
                    EntityIndexConfiguration.builder()
                        .v2(EntityIndexVersionConfiguration.builder().enabled(true).build())
                        .v3(
                            EntityIndexVersionConfiguration.builder()
                                .enabled(true)
                                .keywordReadEnabled(true)
                                .build())
                        .build())
                .build(),
            null,
            QueryFilterRewriteChain.EMPTY,
            TEST_SEARCH_SERVICE_CONFIG);
    OperationContext fulltext = opContext.withSearchFlags(flags -> flags.setFulltext(true));
    List<EntitySpec> datasets = List.of(fulltext.getEntityRegistry().getEntitySpec("dataset"));
    assertNotNull(v3Dao.lightFirstQuery(fulltext, datasets, "orders", null, null));
    // The name-focused light query would hide every dataset that only holds the column
    assertNull(
        v3Dao.lightFirstQuery(
            fulltext, datasets, "orders", null, QueryUtils.newFilter("fieldPaths", "customer_id")));
    assertNull(
        v3Dao.lightFirstQuery(
            fulltext,
            datasets,
            "orders",
            List.of(new SortCriterion().setField("_score").setOrder(SortOrder.ASCENDING)),
            null));
    assertNull(
        v3Dao.lightFirstQuery(
            fulltext,
            datasets,
            "orders",
            List.of(
                new SortCriterion().setField("_score").setOrder(SortOrder.DESCENDING),
                new SortCriterion().setField("urn").setOrder(SortOrder.ASCENDING)),
            null));
    // V2 reads keep the V2 query
    assertNull(dao.lightFirstQuery(fulltext, datasets, "orders", null, null));
    // Quoted, structured, browse-all and URN queries run the full query
    for (String query :
        List.of(
            "\"orders\"",
            SearchQueryBuilder.STRUCTURED_QUERY_PREFIX + "name:orders",
            "*",
            "urn:li:dataset:(urn:li:dataPlatform:hive,orders,PROD)")) {
      assertNull(v3Dao.lightFirstQuery(fulltext, datasets, query, null, null), query);
    }
    // And so does a search that is not full text
    assertNull(
        v3Dao.lightFirstQuery(
            opContext.withSearchFlags(flags -> flags.setFulltext(false)),
            datasets,
            "orders",
            null,
            null));
  }

  @Test
  public void testLightHitsWithoutTotalAreServed() throws Exception {
    SearchResponse untotalled = mock(SearchResponse.class);
    when(untotalled.getHits())
        .thenReturn(new SearchHits(new SearchHit[] {new SearchHit(1)}, null, 0f));
    when(untotalled.getShardFailures()).thenReturn(new ShardSearchFailure[0]);
    when(client.search(any(OperationContext.class), any(), eq(RequestOptions.DEFAULT)))
        .thenReturn(untotalled);
    assertSame(
        dao.searchLightFirst(opContext, request(QueryBuilders.boolQuery().must(FULL)), LIGHT, "x"),
        untotalled);
    verify(client).search(any(OperationContext.class), any(), eq(RequestOptions.DEFAULT));
  }

  private SearchRequest request(QueryBuilder query) {
    return new SearchRequest(
            opContext
                .getSearchContext()
                .getIndexConvention()
                .getEntityIndexName(opContext, "dashboard"))
        .source(new SearchSourceBuilder().query(query));
  }

  private static SearchResponse response(long totalHits, int failedShards) {
    SearchResponse response = mock(SearchResponse.class);
    when(response.getHits())
        .thenReturn(
            new SearchHits(
                new SearchHit[0], new TotalHits(totalHits, TotalHits.Relation.EQUAL_TO), 0f));
    when(response.getFailedShards()).thenReturn(failedShards);
    when(response.getTotalShards()).thenReturn(2);
    when(response.getShardFailures()).thenReturn(new ShardSearchFailure[0]);
    return response;
  }

  @Test
  public void testColumnNameFilter() {
    assertTrue(ESSearchDAO.hasColumnNameFilter(QueryUtils.newFilter("fieldPaths", "customer_id")));
    assertFalse(ESSearchDAO.hasColumnNameFilter(QueryUtils.newFilter("platform", "hive")));
    assertFalse(ESSearchDAO.hasColumnNameFilter(null));
    // Legacy criteria
    Criterion columnName = CriterionUtils.buildCriterion("fieldPaths", Condition.EQUAL, "id");
    assertTrue(
        ESSearchDAO.hasColumnNameFilter(new Filter().setCriteria(new CriterionArray(columnName))));
    // Excluding a column name requires none
    assertFalse(
        ESSearchDAO.hasColumnNameFilter(
            QueryUtils.newFilter(
                CriterionUtils.buildCriterion("fieldPaths", Condition.EQUAL, true, "id"))));
  }

  @Test
  public void testSkipsFullQuery() {
    // Digit runs of six or more are IDs or hashes
    assertTrue(ESSearchDAO.skipsFullQuery("run_20240101_8f3a"));
    // Four or more delimited tokens are a long name
    assertTrue(ESSearchDAO.skipsFullQuery("sales_orders_by_region"));
    assertTrue(ESSearchDAO.skipsFullQuery("my_db.sales.orders.daily"));
    assertFalse(ESSearchDAO.skipsFullQuery("sales_orders"));
    assertFalse(ESSearchDAO.skipsFullQuery("orders 20240101"));
    assertFalse(ESSearchDAO.skipsFullQuery("orders\t20240101"));
    assertFalse(ESSearchDAO.skipsFullQuery("orders\u00A020240101"));
    // A leading delimiter adds no part
    assertFalse(ESSearchDAO.skipsFullQuery("_airbyte_raw_users"));
  }
}
