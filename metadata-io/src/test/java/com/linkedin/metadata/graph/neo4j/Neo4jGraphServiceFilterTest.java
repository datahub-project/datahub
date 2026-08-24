package com.linkedin.metadata.graph.neo4j;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.utils.CriterionUtils;
import java.util.Arrays;
import java.util.List;
import org.testng.annotations.Test;

public class Neo4jGraphServiceFilterTest {

  @Test
  public void filterToFragmentsKeepsSingleValueInPropertyMap() {
    Filter filter =
        andFilter(CriterionUtils.buildCriterion("urn", Condition.EQUAL, "urn:li:domain:root"));

    Neo4jGraphService.Neo4jFilterFragments fragments =
        Neo4jGraphService.filterToFragments(filter, "dest");

    assertEquals(fragments.propertyMap, "{urn:\"urn:li:domain:root\"}");
    assertTrue(fragments.wherePredicates.isEmpty());
  }

  @Test
  public void filterToFragmentsMovesMultiValueEqualToWhereIn() {
    Filter filter =
        andFilter(
            CriterionUtils.buildCriterion(
                "urn", Condition.EQUAL, List.of("urn:li:domain:childA", "urn:li:domain:childB")));

    Neo4jGraphService.Neo4jFilterFragments fragments =
        Neo4jGraphService.filterToFragments(filter, "dest");

    assertEquals(fragments.propertyMap, "");
    assertEquals(
        fragments.wherePredicates,
        List.of("dest.urn IN [\"urn:li:domain:childA\", \"urn:li:domain:childB\"]"));
  }

  @Test
  public void filterToFragmentsSplitsMixedSingleAndMultiValueCriteria() {
    Filter filter =
        andFilter(
            CriterionUtils.buildCriterion("platform", Condition.EQUAL, "mysql"),
            CriterionUtils.buildCriterion(
                "urn", Condition.EQUAL, List.of("urn:li:dataset:a", "urn:li:dataset:b")));

    Neo4jGraphService.Neo4jFilterFragments fragments =
        Neo4jGraphService.filterToFragments(filter, "src");

    assertEquals(fragments.propertyMap, "{platform:\"mysql\"}");
    assertEquals(
        fragments.wherePredicates,
        List.of("src.urn IN [\"urn:li:dataset:a\", \"urn:li:dataset:b\"]"));
  }

  private static Filter andFilter(Criterion... criteria) {
    return new Filter()
        .setOr(
            new ConjunctiveCriterionArray(
                new ConjunctiveCriterion().setAnd(new CriterionArray(Arrays.asList(criteria)))));
  }
}
