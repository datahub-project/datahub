package com.linkedin.metadata.search.elasticsearch.query;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.search.utils.QueryUtils;
import org.opensearch.common.lucene.search.function.CombineFunction;
import org.opensearch.common.lucene.search.function.FunctionScoreQuery;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.opensearch.index.query.functionscore.ScoreFunctionBuilders;
import org.testng.annotations.Test;

public class ESSearchDAOLightFirstTest {

  private static final QueryBuilder FULL = QueryBuilders.matchQuery("name", "full");
  private static final QueryBuilder LIGHT = QueryBuilders.matchQuery("name", "light");
  private static final QueryBuilder FILTER = QueryBuilders.termQuery("platform", "hive");

  @Test
  public void testLightSourceQueryKeepsFilters() {
    BoolQueryBuilder light =
        (BoolQueryBuilder)
            ESSearchDAO.buildLightSourceQuery(
                QueryBuilders.boolQuery().must(FULL).filter(FILTER), LIGHT);
    assertEquals(light.must().size(), 1);
    assertSame(light.must().get(0), LIGHT);
    assertEquals(light.filter().size(), 1);
    assertSame(light.filter().get(0), FILTER);
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
  public void testLightSourceQueryWithoutBoolRoot() {
    assertSame(ESSearchDAO.buildLightSourceQuery(FULL, LIGHT), LIGHT);
  }

  @Test
  public void testColumnNameFilter() {
    assertTrue(ESSearchDAO.hasColumnNameFilter(QueryUtils.newFilter("fieldPaths", "customer_id")));
    assertFalse(ESSearchDAO.hasColumnNameFilter(QueryUtils.newFilter("platform", "hive")));
    assertFalse(ESSearchDAO.hasColumnNameFilter(null));
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
