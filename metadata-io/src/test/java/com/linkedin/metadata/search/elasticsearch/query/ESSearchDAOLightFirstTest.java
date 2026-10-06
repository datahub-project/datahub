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
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.elasticsearch.query.filter.QueryFilterRewriteChain;
import com.linkedin.metadata.search.utils.QueryUtils;
import com.linkedin.metadata.utils.CriterionUtils;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
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
  }
}
