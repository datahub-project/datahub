package com.linkedin.metadata.graph.neo4j;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.linkedin.data.template.StringArray;
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

  @Test
  public void filterToFragmentsRejectsEmptyEqualValues() {
    Criterion empty =
        new Criterion().setField("urn").setCondition(Condition.EQUAL).setValues(new StringArray());
    Filter filter = andFilter(empty);

    assertThrows(
        IllegalArgumentException.class, () -> Neo4jGraphService.filterToFragments(filter, "src"));
  }

  @Test
  public void appendWherePredicatesParenthesizesExistingOrBody() {
    String where =
        " WHERE left(type(r), 2)<>'r_' AND (src:dataset OR src:chart) AND (dest:dataset)";
    String result =
        Neo4jGraphService.appendWherePredicates(
            where, List.of("dest.urn IN [\"urn:li:dataset:a\", \"urn:li:dataset:b\"]"));

    assertEquals(
        result,
        " WHERE (left(type(r), 2)<>'r_' AND (src:dataset OR src:chart) AND (dest:dataset))"
            + " AND dest.urn IN [\"urn:li:dataset:a\", \"urn:li:dataset:b\"]");
  }

  @Test
  public void commonSourceNodeLabelRequiresUniformEntityType() {
    assertEquals(
        Neo4jGraphService.commonSourceNodeLabel(
            andFilter(
                CriterionUtils.buildCriterion(
                    "urn", Condition.EQUAL, List.of("urn:li:domain:a", "urn:li:domain:b")))),
        "domain");

    assertNull(
        Neo4jGraphService.commonSourceNodeLabel(
            andFilter(
                CriterionUtils.buildCriterion(
                    "urn", Condition.EQUAL, List.of("urn:li:domain:a", "urn:li:dataset:b")))));

    assertEquals(
        Neo4jGraphService.commonSourceNodeLabel(
            andFilter(
                CriterionUtils.buildCriterion("platform", Condition.EQUAL, "mysql"),
                CriterionUtils.buildCriterion("urn", Condition.EQUAL, "urn:li:dataset:a"))),
        "dataset");
  }

  private static Filter andFilter(Criterion... criteria) {
    return new Filter()
        .setOr(
            new ConjunctiveCriterionArray(
                new ConjunctiveCriterion().setAnd(new CriterionArray(Arrays.asList(criteria)))));
  }
}
