package com.linkedin.metadata.search.utils;

import static com.linkedin.metadata.search.utils.ESUtils.KEYWORD_SUFFIX;
import static com.linkedin.metadata.search.utils.ESUtils.LIFECYCLE_STAGE;
import static com.linkedin.metadata.search.utils.ESUtils.REMOVED;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.datahub.authorization.config.ViewAuthorizationConfiguration;
import com.linkedin.metadata.query.SearchFlags;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.OperationContextConfig;
import io.datahubproject.metadata.context.SearchContext;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.testng.annotations.Test;

/** Tests for the lifecycle stage filtering logic in {@link ESUtils#applyDefaultSearchFilters}. */
public class ESUtilsLifecycleStageFilterTest {

  private static final String PROPOSED_URN = "urn:li:lifecycleStageType:PROPOSED";
  private static final String ARCHIVED_URN = "urn:li:lifecycleStageType:ARCHIVED";

  @Test
  public void testHiddenStagesExcluded() {
    OperationContext opContext = mockOpContext(new SearchFlags().setIncludeSoftDeleted(false));
    BoolQueryBuilder query = QueryBuilders.boolQuery();

    ESUtils.applyDefaultSearchFilters(
        opContext, List.of("document"), null, query, Set.of(PROPOSED_URN));

    // Should have mustNot for removed=true AND mustNot for lifecycleStage=PROPOSED
    assertEquals(query.mustNot().size(), 2);
    assertMustNotContainsTerm(query, REMOVED, "true");
    assertMustNotContainsTerm(query, LIFECYCLE_STAGE + KEYWORD_SUFFIX, PROPOSED_URN);
  }

  @Test
  public void testMultipleHiddenStages() {
    OperationContext opContext = mockOpContext(new SearchFlags().setIncludeSoftDeleted(false));
    BoolQueryBuilder query = QueryBuilders.boolQuery();

    ESUtils.applyDefaultSearchFilters(
        opContext, List.of("document"), null, query, Set.of(PROPOSED_URN, ARCHIVED_URN));

    // removed + 2 lifecycle stages = 3 mustNot clauses
    assertEquals(query.mustNot().size(), 3);
    assertMustNotContainsTerm(query, LIFECYCLE_STAGE + KEYWORD_SUFFIX, PROPOSED_URN);
    assertMustNotContainsTerm(query, LIFECYCLE_STAGE + KEYWORD_SUFFIX, ARCHIVED_URN);
  }

  @Test
  public void testEmptyHiddenStages_noLifecycleFilter() {
    OperationContext opContext = mockOpContext(new SearchFlags().setIncludeSoftDeleted(false));
    BoolQueryBuilder query = QueryBuilders.boolQuery();

    ESUtils.applyDefaultSearchFilters(
        opContext, List.of("document"), null, query, Collections.emptySet());

    // Only removed=true mustNot, no lifecycle stage filter
    assertEquals(query.mustNot().size(), 1);
    assertMustNotContainsTerm(query, REMOVED, "true");
  }

  @Test
  public void testIncludeHiddenLifecycleStages_skipsFilter() {
    SearchFlags flags =
        new SearchFlags().setIncludeSoftDeleted(false).setIncludeHiddenLifecycleStages(true);
    OperationContext opContext = mockOpContext(flags);
    BoolQueryBuilder query = QueryBuilders.boolQuery();

    ESUtils.applyDefaultSearchFilters(
        opContext, List.of("document"), null, query, Set.of(PROPOSED_URN));

    // Only removed=true, no lifecycle stage filter because includeHiddenLifecycleStages=true
    assertEquals(query.mustNot().size(), 1);
    assertMustNotContainsTerm(query, REMOVED, "true");
  }

  @Test
  public void testExplicitLifecycleStageFilter_skipsDefault() {
    OperationContext opContext = mockOpContext(new SearchFlags().setIncludeSoftDeleted(false));

    // User already filtering on lifecycleStage field
    Criterion stageCriterion = new Criterion();
    stageCriterion.setField(LIFECYCLE_STAGE);
    stageCriterion.setCondition(Condition.EQUAL);

    ConjunctiveCriterion conj = new ConjunctiveCriterion();
    conj.setAnd(new CriterionArray(stageCriterion));

    Filter filter = new Filter();
    filter.setOr(new ConjunctiveCriterionArray(conj));

    BoolQueryBuilder query = QueryBuilders.boolQuery();
    ESUtils.applyDefaultSearchFilters(
        opContext, List.of("document"), filter, query, Set.of(PROPOSED_URN));

    // Only removed=true mustNot, no lifecycle stage filter (user overrides)
    assertEquals(query.mustNot().size(), 1);
    assertMustNotContainsTerm(query, REMOVED, "true");
  }

  @Test
  public void testIncludeSoftDeleted_skipsRemovedFilter() {
    SearchFlags flags = new SearchFlags().setIncludeSoftDeleted(true);
    OperationContext opContext = mockOpContext(flags);
    BoolQueryBuilder query = QueryBuilders.boolQuery();

    ESUtils.applyDefaultSearchFilters(
        opContext, List.of("document"), null, query, Set.of(PROPOSED_URN));

    // Only lifecycle stage filter, no removed filter
    assertEquals(query.mustNot().size(), 1);
    assertMustNotContainsTerm(query, LIFECYCLE_STAGE + KEYWORD_SUFFIX, PROPOSED_URN);
  }

  // ── Helpers ─────────────────────────────────────────────────────────────────

  private static OperationContext mockOpContext(SearchFlags searchFlags) {
    SearchContext searchContext = mock(SearchContext.class);
    when(searchContext.getSearchFlags()).thenReturn(searchFlags);

    ViewAuthorizationConfiguration viewAuthConfig =
        ViewAuthorizationConfiguration.builder().enabled(false).build();
    OperationContextConfig opConfig = mock(OperationContextConfig.class);
    when(opConfig.getViewAuthorizationConfiguration()).thenReturn(viewAuthConfig);

    OperationContext opContext = mock(OperationContext.class);
    when(opContext.getSearchContext()).thenReturn(searchContext);
    when(opContext.isSystemAuth()).thenReturn(true);
    when(opContext.getOperationContextConfig()).thenReturn(opConfig);
    return opContext;
  }

  private static void assertMustNotContainsTerm(
      BoolQueryBuilder query, String field, String value) {
    String queryString = query.toString();
    assertTrue(
        queryString.contains(field) && queryString.contains(value),
        String.format(
            "Expected mustNot to contain term %s=%s, but query was: %s",
            field, value, queryString));
  }
}
