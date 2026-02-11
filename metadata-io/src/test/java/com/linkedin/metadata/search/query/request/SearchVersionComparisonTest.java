package com.linkedin.metadata.search.query.request;

import static io.datahubproject.test.search.SearchTestUtils.TEST_OS_SEARCH_CONFIG;
import static org.testng.Assert.*;

import com.google.common.collect.ImmutableList;
import com.linkedin.metadata.TestEntitySpecBuilder;
import com.linkedin.metadata.config.search.SearchConfiguration;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.search.elasticsearch.query.request.SearchQueryBuilder;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.List;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.DisMaxQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.testng.annotations.Test;

/**
 * Test to demonstrate the differences between Search V2 and V2.5 with the query "wau" (a typo that
 * should fuzzy-match to similar terms).
 */
public class SearchVersionComparisonTest {

  private static final String TEST_QUERY = "wau";

  private SearchQueryBuilder createBuilder(boolean enableV2_5) {
    SearchConfiguration config = TEST_OS_SEARCH_CONFIG.getSearch();
    config.setEnableSearchV2_5(enableV2_5);
    return new SearchQueryBuilder(config, null);
  }

  private OperationContext createOpContext(Boolean perRequestOverride) {
    OperationContext baseContext = TestOperationContexts.systemContextNoSearchAuthorization();

    if (perRequestOverride != null) {
      return baseContext.withSearchFlags(flags -> flags.setSearchVersionV2_5(perRequestOverride));
    }

    return baseContext;
  }

  @Test
  public void testV2_UsesExactTermsAndAND() {
    SearchQueryBuilder builderV2 = createBuilder(false);
    OperationContext opContext = createOpContext(null);
    List<EntitySpec> entitySpecs = ImmutableList.of(TestEntitySpecBuilder.getSpec());

    QueryBuilder query = builderV2.buildQuery(opContext, entitySpecs, TEST_QUERY, true);
    String queryString = query.toString();

    // V2 should NOT have fuzzy operators
    assertFalse(queryString.contains("~1"), "V2 should not have ~1 fuzzy operator");
    assertFalse(queryString.contains("~2"), "V2 should not have ~2 fuzzy operator");

    // V2 uses BoolQueryBuilder for the main query
    FunctionScoreQueryBuilder functionScore = (FunctionScoreQueryBuilder) query;
    QueryBuilder innerQuery = functionScore.query();
    assertTrue(innerQuery instanceof BoolQueryBuilder, "V2 should use BoolQueryBuilder structure");
  }

  @Test
  public void testV2_5_WithoutFuzzy() {
    SearchQueryBuilder builderV2_5 = createBuilder(true);
    OperationContext opContext = createOpContext(null);
    List<EntitySpec> entitySpecs = ImmutableList.of(TestEntitySpecBuilder.getSpec());

    QueryBuilder query = builderV2_5.buildQuery(opContext, entitySpecs, TEST_QUERY, true);
    String queryString = query.toString();

    // V2.5 with fuzzy disabled should NOT have fuzzy operators
    assertFalse(
        queryString.contains("~1") || queryString.contains("~2"),
        "V2.5 with fuzzy disabled should not have fuzzy operators");

    // V2.5 uses DisMaxQueryBuilder
    FunctionScoreQueryBuilder functionScore = (FunctionScoreQueryBuilder) query;
    QueryBuilder innerQuery = functionScore.query();
    assertTrue(
        innerQuery instanceof DisMaxQueryBuilder, "V2.5 should use DisMaxQueryBuilder structure");
  }

  @Test
  public void testPerRequestOverride_V2_5() {
    // Server config: V2, but per-request override to V2.5
    SearchQueryBuilder builderV2 = createBuilder(false);
    OperationContext opContext = createOpContext(true);
    List<EntitySpec> entitySpecs = ImmutableList.of(TestEntitySpecBuilder.getSpec());

    QueryBuilder query = builderV2.buildQuery(opContext, entitySpecs, TEST_QUERY, true);

    // Should use V2.5 despite server config being V2
    FunctionScoreQueryBuilder functionScore = (FunctionScoreQueryBuilder) query;
    assertTrue(
        functionScore.query() instanceof DisMaxQueryBuilder,
        "Per-request override should use V2.5 DisMaxQueryBuilder");
  }

  @Test
  public void testPerRequestOverride_V2() {
    // Server config: V2.5, but per-request override to V2
    SearchQueryBuilder builderV2_5 = createBuilder(true);
    OperationContext opContext = createOpContext(false);
    List<EntitySpec> entitySpecs = ImmutableList.of(TestEntitySpecBuilder.getSpec());

    QueryBuilder query = builderV2_5.buildQuery(opContext, entitySpecs, TEST_QUERY, true);
    String queryString = query.toString();

    // Should use V2 despite server config being V2.5
    assertFalse(queryString.contains("~1"), "Per-request override should disable fuzzy");
    assertFalse(queryString.contains("~2"), "Per-request override should disable fuzzy");

    FunctionScoreQueryBuilder functionScore = (FunctionScoreQueryBuilder) query;
    assertTrue(
        functionScore.query() instanceof BoolQueryBuilder,
        "Per-request override should use V2 BoolQueryBuilder");
  }

  @Test
  public void testQueryStructureDifference() {
    SearchQueryBuilder builderV2 = createBuilder(false);
    SearchQueryBuilder builderV2_5 = createBuilder(true);
    OperationContext opContext = createOpContext(null);
    List<EntitySpec> entitySpecs = ImmutableList.of(TestEntitySpecBuilder.getSpec());

    QueryBuilder queryV2 = builderV2.buildQuery(opContext, entitySpecs, TEST_QUERY, true);
    QueryBuilder queryV2_5 = builderV2_5.buildQuery(opContext, entitySpecs, TEST_QUERY, true);

    String v2String = queryV2.toString();
    String v2_5String = queryV2_5.toString();

    assertNotEquals(v2String, v2_5String, "V2 and V2.5 queries should be different");

    // V2 uses BoolQueryBuilder
    assertTrue(((FunctionScoreQueryBuilder) queryV2).query() instanceof BoolQueryBuilder);

    // V2.5 uses DisMaxQueryBuilder
    assertTrue(((FunctionScoreQueryBuilder) queryV2_5).query() instanceof DisMaxQueryBuilder);
  }
}
