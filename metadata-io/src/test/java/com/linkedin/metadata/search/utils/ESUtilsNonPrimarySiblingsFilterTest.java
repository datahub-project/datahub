package com.linkedin.metadata.search.utils;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.data.template.StringArray;
import com.linkedin.metadata.config.search.SearchConfiguration;
import com.linkedin.metadata.query.SearchFlags;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import javax.annotation.Nullable;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.testng.annotations.Test;

/** {@link ESUtils#filterNonPrimarySiblings} hides non-primary dataset siblings when enabled. */
public class ESUtilsNonPrimarySiblingsFilterTest {

  @Test
  public void testDisabledIsNoOp() {
    BoolQueryBuilder query = apply(false, null);
    assertTrue(query.mustNot().isEmpty(), query.toString());
  }

  @Test
  public void testMissingConfigurationIsNoOp() {
    BoolQueryBuilder query =
        ESUtils.filterNonPrimarySiblings(null, QueryBuilders.boolQuery(), null);
    assertTrue(query.mustNot().isEmpty(), query.toString());
  }

  @Test
  public void testEnabledHidesNonPrimaryDatasetSiblings() {
    BoolQueryBuilder query = apply(true, null);
    assertEquals(query.mustNot().size(), 1, query.toString());
    String clause = query.mustNot().get(0).toString();
    assertTrue(clause.contains("\"urn:li:dataset:\""), clause);
    assertTrue(clause.contains("\"hasSiblings\""), clause);
    assertTrue(clause.contains("\"siblingsPrimary\""), clause);
  }

  /**
   * With search text, a non-primary is hidden only when the text also matches its primary's name,
   * because the query may match only the non-primary member.
   */
  @Test
  public void testSearchTextHidesOnlyWhenPrimaryNameMatches() {
    SearchConfiguration config = new SearchConfiguration().setHideNonPrimarySiblings(true);
    BoolQueryBuilder withText =
        ESUtils.filterNonPrimarySiblings(null, "hourly_kpi", QueryBuilders.boolQuery(), config);
    assertEquals(withText.mustNot().size(), 1, withText.toString());
    String clause = withText.mustNot().get(0).toString();
    assertTrue(clause.contains("\"siblingPrimaryName\""), clause);
    assertTrue(clause.contains("\"hourly_kpi\""), clause);
    assertTrue(clause.contains("\"AND\""), clause);
    for (String matchAll : new String[] {null, "", "  ", "*", " * "}) {
      BoolQueryBuilder query =
          ESUtils.filterNonPrimarySiblings(null, matchAll, QueryBuilders.boolQuery(), config);
      assertEquals(query.mustNot().size(), 1, "query '" + matchAll + "': " + query);
      assertTrue(
          !query.mustNot().get(0).toString().contains("siblingPrimaryName"),
          "query '" + matchAll + "': " + query);
    }
  }

  /** Autocomplete matches the primary's name by prefix, as it matches the primary itself. */
  @Test
  public void testAutocompleteMatchesThePrimaryNameByPrefix() {
    SearchConfiguration config = new SearchConfiguration().setHideNonPrimarySiblings(true);
    BoolQueryBuilder prefix =
        ESUtils.filterNonPrimarySiblings(
            null, null, "ord", true, QueryBuilders.boolQuery(), config);
    assertTrue(prefix.mustNot().get(0).toString().contains("match_bool_prefix"), prefix.toString());
    assertTrue(prefix.mustNot().get(0).toString().contains("\"AND\""), prefix.toString());
    BoolQueryBuilder full =
        ESUtils.filterNonPrimarySiblings(
            null, null, "ord", false, QueryBuilders.boolQuery(), config);
    assertTrue(!full.mustNot().get(0).toString().contains("match_bool_prefix"), full.toString());
  }

  /** A negated term can exclude the primary while its member matches, so nothing is hidden. */
  @Test
  public void testNegatedSearchTextTurnsHidingOff() {
    SearchConfiguration config = new SearchConfiguration().setHideNonPrimarySiblings(true);
    for (String negated :
        new String[] {"kpi -daily", "-daily kpi", "kpi !daily", "kpi NOT daily"}) {
      BoolQueryBuilder query =
          ESUtils.filterNonPrimarySiblings(
              null, null, negated, false, QueryBuilders.boolQuery(), config);
      assertTrue(query.mustNot().isEmpty(), negated + ": " + query);
    }
    // A hyphen inside a name is not a negation.
    BoolQueryBuilder hyphenated =
        ESUtils.filterNonPrimarySiblings(
            null, null, "hourly-kpi", false, QueryBuilders.boolQuery(), config);
    assertEquals(hyphenated.mustNot().size(), 1, hyphenated.toString());
  }

  /** API listings and upgrade jobs set this flag to see every member. */
  @Test
  public void testIncludeNonPrimarySiblingsFlagTurnsHidingOff() {
    SearchConfiguration config = new SearchConfiguration().setHideNonPrimarySiblings(true);
    BoolQueryBuilder optedOut =
        ESUtils.filterNonPrimarySiblings(
            new SearchFlags().setIncludeNonPrimarySiblings(true),
            null,
            null,
            QueryBuilders.boolQuery(),
            config);
    assertTrue(optedOut.mustNot().isEmpty(), optedOut.toString());
    BoolQueryBuilder defaults =
        ESUtils.filterNonPrimarySiblings(
            new SearchFlags(), null, null, QueryBuilders.boolQuery(), config);
    assertEquals(defaults.mustNot().size(), 1, defaults.toString());
  }

  @Test
  public void testEntityTypeAndSoftDeleteFiltersStillHide() {
    for (Filter filter :
        new Filter[] {
          filterOn("_entityType", "DATASET"),
          // What V2 search turns an _entityType criterion into before hiding is applied.
          filterOn("_index", "datasetindex_v2"),
          filterOn("entity", "DATASET"),
          filterOn("removed", "false"),
          filterOn("_entityType\u241EtypeNames", "DATASET"),
          legacyFilterOn("_entityType", "DATASET")
        }) {
      BoolQueryBuilder query = apply(true, filter);
      assertEquals(query.mustNot().size(), 1, filter + ": " + query);
    }
  }

  /** The sibling matching such a filter may be the non-primary one, so nothing is hidden. */
  @Test
  public void testOtherFiltersTurnHidingOff() {
    for (Filter filter :
        new Filter[] {
          filterOn("platform", "urn:li:dataPlatform:snowflake"),
          filterOn("typeNames", "Table"),
          filterOn("_entityType\u241EtypeNames", "DATASET\u241ETable"),
          legacyFilterOn("platform.keyword", "urn:li:dataPlatform:snowflake")
        }) {
      BoolQueryBuilder query = apply(true, filter);
      assertTrue(query.mustNot().isEmpty(), filter + ": " + query);
    }
  }

  @Test
  public void testOneNonNeutralCriterionAmongNeutralOnesTurnsHidingOff() {
    Filter filter =
        new Filter()
            .setOr(
                new ConjunctiveCriterionArray(
                    new ConjunctiveCriterion()
                        .setAnd(
                            new CriterionArray(
                                criterion("_entityType", "DATASET"),
                                criterion("platform", "urn:li:dataPlatform:snowflake")))));
    assertTrue(apply(true, filter).mustNot().isEmpty());
  }

  /**
   * The shape GraphQL search adds to every request that includes documents: (lifecycleStage =
   * PUBLISHED AND showInGlobalContext != false) OR (lifecycleStage IS NULL AND state != UNPUBLISHED
   * AND showInGlobalContext != false).
   */
  private static Filter documentVisibilityDefaults(Criterion... extra) {
    CriterionArray published =
        new CriterionArray(
            criterion("lifecycleStage", "urn:li:lifecycleStageType:PUBLISHED"),
            criterion("showInGlobalContext", "false").setNegated(true));
    CriterionArray legacy =
        new CriterionArray(
            new Criterion().setField("lifecycleStage").setCondition(Condition.IS_NULL),
            criterion("state", "UNPUBLISHED").setNegated(true),
            criterion("showInGlobalContext", "false").setNegated(true));
    for (Criterion c : extra) {
      published.add(c);
      legacy.add(c);
    }
    return new Filter()
        .setOr(
            new ConjunctiveCriterionArray(
                new ConjunctiveCriterion().setAnd(published),
                new ConjunctiveCriterion().setAnd(legacy)));
  }

  @Test
  public void testDocumentVisibilityDefaultsStillHide() {
    BoolQueryBuilder query = apply(true, documentVisibilityDefaults());
    assertEquals(query.mustNot().size(), 1, query.toString());
  }

  @Test
  public void testUserFilterCombinedWithDocumentDefaultsTurnsHidingOff() {
    Filter filter =
        documentVisibilityDefaults(criterion("platform", "urn:li:dataPlatform:snowflake"));
    assertTrue(apply(true, filter).mustNot().isEmpty());
  }

  /** A default view made only of exclusions, e.g. "tag is not deprecated", keeps hiding on. */
  @Test
  public void testNegatedCriteriaStillHide() {
    Filter filter =
        documentVisibilityDefaults(
            criterion("tags", "urn:li:tag:deprecated").setNegated(true),
            criterion("deprecated", "true").setNegated(true));
    assertEquals(apply(true, filter).mustNot().size(), 1);
  }

  /** A negated IS_NULL means "has a value", which selects. */
  @Test
  public void testNegatedIsNullTurnsHidingOff() {
    Filter filter =
        new Filter()
            .setOr(
                new ConjunctiveCriterionArray(
                    new ConjunctiveCriterion()
                        .setAnd(
                            new CriterionArray(
                                new Criterion()
                                    .setField("description")
                                    .setCondition(Condition.IS_NULL)
                                    .setNegated(true)))));
    assertTrue(apply(true, filter).mustNot().isEmpty());
  }

  /** "Does not exist" (a negated EXISTS) selects documents lacking the field. */
  @Test
  public void testNegatedExistsTurnsHidingOff() {
    Filter filter =
        new Filter()
            .setOr(
                new ConjunctiveCriterionArray(
                    new ConjunctiveCriterion()
                        .setAnd(
                            new CriterionArray(
                                new Criterion()
                                    .setField("description")
                                    .setCondition(Condition.EXISTS)
                                    .setNegated(true)))));
    assertTrue(apply(true, filter).mustNot().isEmpty());
  }

  @Test
  public void testNegatedSiblingFieldIsExempt() {
    Filter filter = filterOn("hasSiblings", "true");
    filter.getOr().get(0).getAnd().get(0).setNegated(true);
    assertTrue(apply(true, filter).mustNot().isEmpty());
  }

  @Test
  public void testFilterOnSiblingFieldsIsExempt() {
    for (String field : new String[] {"siblings", "hasSiblings", "siblingsPrimary.keyword"}) {
      BoolQueryBuilder query = apply(true, filterOn(field, "x"));
      assertTrue(query.mustNot().isEmpty(), field + ": " + query);
    }
  }

  private static BoolQueryBuilder apply(boolean enabled, @Nullable Filter filter) {
    SearchConfiguration config = new SearchConfiguration().setHideNonPrimarySiblings(enabled);
    return ESUtils.filterNonPrimarySiblings(filter, QueryBuilders.boolQuery(), config);
  }

  private static Criterion criterion(String field, String value) {
    return new Criterion()
        .setField(field)
        .setCondition(Condition.EQUAL)
        .setValues(new StringArray(value));
  }

  private static Filter filterOn(String field, String value) {
    return new Filter()
        .setOr(
            new ConjunctiveCriterionArray(
                new ConjunctiveCriterion().setAnd(new CriterionArray(criterion(field, value)))));
  }

  private static Filter legacyFilterOn(String field, String value) {
    return new Filter().setCriteria(new CriterionArray(criterion(field, value)));
  }
}
