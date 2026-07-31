package com.linkedin.metadata.utils;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.Criterion;
import java.util.Collections;
import java.util.List;
import org.testng.annotations.Test;

public class CriterionUtilsTest {

  @Test
  public void testBuildCriterionWithStrings() {
    Criterion c = CriterionUtils.buildCriterion("field", Condition.EQUAL, "a", "b");
    assertEquals(c.getField(), "field");
    assertEquals(c.getCondition(), Condition.EQUAL);
    assertEquals(c.getValues().size(), 2);
    assertEquals(c.getValues().get(0), "a");
    assertEquals(c.getValues().get(1), "b");
  }

  @Test
  public void testBuildCriterionDedupesValues() {
    Criterion c = CriterionUtils.buildCriterion("field", Condition.EQUAL, "x", "x");
    assertEquals(c.getValues().size(), 1);
    assertEquals(c.getValues().get(0), "x");
  }

  @Test
  public void testBuildExistsCriterion() {
    Criterion c = CriterionUtils.buildExistsCriterion("f");
    assertEquals(c.getCondition(), Condition.EXISTS);
    assertEquals(c.getValues().size(), 0);
  }

  @Test
  public void testBuildConjunctiveCriterion() {
    assertNotNull(
        CriterionUtils.buildConjunctiveCriterion(
            CriterionUtils.buildCriterion("a", Condition.EQUAL, "1"),
            CriterionUtils.buildCriterion("b", Condition.EQUAL, "2")));
  }

  @Test
  public void testBuildCriterionEmptyCollection() {
    Criterion c =
        CriterionUtils.buildCriterion("field", Condition.EQUAL, false, Collections.emptyList());
    assertEquals(c.getValues().size(), 0);
  }

  @Test
  public void testBuildNotExistsCriterion() {
    Criterion c = CriterionUtils.buildNotExistsCriterion("f");
    assertEquals(c.getField(), "f");
    assertEquals(c.getCondition(), Condition.EXISTS);
    assertTrue(c.isNegated());
    assertEquals(c.getValues().size(), 0);
  }

  @Test
  public void testBuildIsNullCriterion() {
    Criterion c = CriterionUtils.buildIsNullCriterion("col");
    assertEquals(c.getCondition(), Condition.IS_NULL);
    assertFalse(c.isNegated());
  }

  @Test
  public void testBuildIsNotNullCriterion() {
    Criterion c = CriterionUtils.buildIsNotNullCriterion("col");
    assertEquals(c.getCondition(), Condition.IS_NULL);
    assertTrue(c.isNegated());
  }

  @Test
  public void testBuildCriterionWithCollection() {
    Criterion c = CriterionUtils.buildCriterion("field", Condition.EQUAL, List.of("a", "b"));
    assertEquals(c.getValues().size(), 2);
    assertEquals(c.getValues().get(0), "a");
  }
}
