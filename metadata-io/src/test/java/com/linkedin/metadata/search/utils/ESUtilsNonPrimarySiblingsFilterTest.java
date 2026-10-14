package com.linkedin.metadata.search.utils;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.data.template.StringArray;
import com.linkedin.metadata.config.search.SearchConfiguration;
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

  @Test
  public void testUnrelatedFilterStillHides() {
    BoolQueryBuilder query = apply(true, filterOn("platform"));
    assertFalse(query.mustNot().isEmpty(), query.toString());
  }

  @Test
  public void testFilterOnSiblingFieldsIsExempt() {
    for (String field : new String[] {"siblings", "hasSiblings", "siblingsPrimary.keyword"}) {
      BoolQueryBuilder query = apply(true, filterOn(field));
      assertTrue(query.mustNot().isEmpty(), field + ": " + query);
    }
  }

  private static BoolQueryBuilder apply(boolean enabled, @Nullable Filter filter) {
    SearchConfiguration config = new SearchConfiguration().setHideNonPrimarySiblings(enabled);
    return ESUtils.filterNonPrimarySiblings(filter, QueryBuilders.boolQuery(), config);
  }

  private static Filter filterOn(String field) {
    Criterion criterion =
        new Criterion()
            .setField(field)
            .setCondition(Condition.EQUAL)
            .setValues(new StringArray("x"));
    return new Filter()
        .setOr(
            new ConjunctiveCriterionArray(
                new ConjunctiveCriterion().setAnd(new CriterionArray(criterion))));
  }
}
