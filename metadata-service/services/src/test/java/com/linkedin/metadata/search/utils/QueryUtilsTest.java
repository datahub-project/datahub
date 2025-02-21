package com.linkedin.metadata.search.utils;

import com.linkedin.data.template.StringArray;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.Filter;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.Test;

public class QueryUtilsTest {

  @Test
  public void testNewCriterionEmptyValues() {
    List<String> emptyList = Collections.emptyList();
    // Should return null when the values list is empty.
    Criterion crit = QueryUtils.newCriterion("testField", emptyList);
    Assert.assertNull(crit, "Expected null when values list is empty");
  }

  @Test
  public void testNewCriterionWithValues() {
    List<String> values = Arrays.asList("val1", "val2");
    Criterion crit = QueryUtils.newCriterion("testField", values);
    Assert.assertNotNull(crit, "Criterion should not be null");
    Assert.assertEquals(crit.getField(), "testField", "Field name should match");
    Assert.assertEquals(crit.getValue(), "val1", "Value should be set to the first element");
    // Assuming StringArray has a method getElements() returning a List<String>
    Assert.assertEquals(
        crit.getValues(), new StringArray(values), "Values should match the input list");
    Assert.assertEquals(
        crit.getCondition(), Condition.EQUAL, "Condition should be EQUAL by default");
  }

  @Test
  public void testNewCriterionWithProvidedCondition() {
    List<String> values = Arrays.asList("val1");
    // Here we explicitly pass the condition (using EQUAL in this case)
    Criterion crit = QueryUtils.newCriterion("field", values, Condition.EQUAL);
    Assert.assertNotNull(crit, "Criterion should not be null");
    Assert.assertEquals(crit.getField(), "field", "Field name should match");
    Assert.assertEquals(crit.getValue(), "val1", "Value should be the first element");
    Assert.assertEquals(
        crit.getValues(), new StringArray(values), "Values should match the input list");
    Assert.assertEquals(
        crit.getCondition(), Condition.EQUAL, "Condition should match the provided condition");
  }

  @Test
  public void testNewFilterWithNullMap() {
    // When params is null, the returned filter should be the EMPTY_FILTER
    Filter filter = QueryUtils.newFilter((Map<String, String>) null);
    Assert.assertSame(filter, QueryUtils.EMPTY_FILTER, "Expected EMPTY_FILTER when params is null");
  }

  @Test
  public void testNewFilterWithEmptyMap() {
    // When an empty map is provided, we expect a filter whose inner criteria list is empty.
    Filter filter = QueryUtils.newFilter(new HashMap<>());
    Assert.assertNotNull(filter.getOr(), "Filter 'or' clause should not be null");
    Assert.assertEquals(filter.getOr().size(), 1, "Expected one conjunctive criterion");
    ConjunctiveCriterion cc = filter.getOr().get(0);
    Assert.assertNotNull(cc.getAnd(), "ConjunctiveCriterion 'and' clause should not be null");
    Assert.assertTrue(cc.getAnd().isEmpty(), "Expected empty criteria list for empty params map");
  }

  @Test
  public void testNewFilterWithValidEntry() {
    Map<String, String> params = new HashMap<>();
    params.put("field1", "value1");
    Filter filter = QueryUtils.newFilter(params);

    // Verify the internal structure: one ConjunctiveCriterion containing one Criterion.
    Assert.assertNotNull(filter.getOr(), "Filter 'or' clause should not be null");
    Assert.assertEquals(filter.getOr().size(), 1, "Expected one conjunctive criterion");
    ConjunctiveCriterion cc = filter.getOr().get(0);
    Assert.assertNotNull(cc.getAnd(), "ConjunctiveCriterion 'and' clause should not be null");
    Assert.assertEquals(cc.getAnd().size(), 1, "Expected one criterion in the filter");
    Criterion crit = cc.getAnd().get(0);
    Assert.assertEquals(crit.getField(), "field1", "Field name should match");
    Assert.assertEquals(crit.getValue(), "", "Value should be empty string");
    Assert.assertEquals(
        crit.getValues(),
        new StringArray(Collections.singletonList("value1")),
        "Values should contain the provided value");
    Assert.assertEquals(crit.getCondition(), Condition.EQUAL, "Condition should be EQUAL");
  }

  @Test
  public void testNewListsFilterWithNullMap() {
    // When params is null, the returned filter should be the EMPTY_FILTER.
    Filter filter = QueryUtils.newListsFilter(null);
    Assert.assertSame(filter, QueryUtils.EMPTY_FILTER, "Expected EMPTY_FILTER when params is null");
  }

  @Test
  public void testNewListsFilterWithEmptyMap() {
    Filter filter = QueryUtils.newListsFilter(new HashMap<>());
    Assert.assertNotNull(filter.getOr(), "Filter 'or' clause should not be null");
    Assert.assertEquals(filter.getOr().size(), 1, "Expected one conjunctive criterion");
    ConjunctiveCriterion cc = filter.getOr().get(0);
    Assert.assertNotNull(cc.getAnd(), "ConjunctiveCriterion 'and' clause should not be null");
    Assert.assertTrue(cc.getAnd().isEmpty(), "Expected empty criteria list for empty params map");
  }

  @Test
  public void testNewListsFilterWithValidEntry() {
    Map<String, List<String>> params = new HashMap<>();
    params.put("field2", Arrays.asList("a", "b"));
    Filter filter = QueryUtils.newListsFilter(params);

    Assert.assertNotNull(filter.getOr(), "Filter 'or' clause should not be null");
    Assert.assertEquals(filter.getOr().size(), 1, "Expected one conjunctive criterion");
    ConjunctiveCriterion cc = filter.getOr().get(0);
    Assert.assertNotNull(cc.getAnd(), "ConjunctiveCriterion 'and' clause should not be null");
    Assert.assertEquals(cc.getAnd().size(), 1, "Expected one criterion in the filter");
    Criterion crit = cc.getAnd().get(0);
    Assert.assertEquals(crit.getField(), "field2", "Field name should match");
    Assert.assertEquals(
        crit.getValue(), "a", "Value should be set to the first element of the list");
    Assert.assertEquals(
        crit.getValues(),
        new StringArray(Arrays.asList("a", "b")),
        "Values should match the input list");
    Assert.assertEquals(crit.getCondition(), Condition.EQUAL, "Condition should be EQUAL");
  }

  @Test
  public void testNewFilterWithFieldAndValue() {
    // This method is just a convenience overload that should be equivalent
    // to newFilter(Collections.singletonMap(field, value))
    Filter filter = QueryUtils.newFilter("field3", "value3");

    Assert.assertNotNull(filter.getOr(), "Filter 'or' clause should not be null");
    Assert.assertEquals(filter.getOr().size(), 1, "Expected one conjunctive criterion");
    ConjunctiveCriterion cc = filter.getOr().get(0);
    Assert.assertNotNull(cc.getAnd(), "ConjunctiveCriterion 'and' clause should not be null");
    Assert.assertEquals(cc.getAnd().size(), 1, "Expected one criterion in the filter");
    Criterion crit = cc.getAnd().get(0);
    Assert.assertEquals(crit.getField(), "field3", "Field name should match");
    Assert.assertEquals(
        crit.getValues(),
        new StringArray(Collections.singletonList("value3")),
        "Values should contain the provided value");
    Assert.assertEquals(crit.getValue(), "", "Value should be empty string");
    Assert.assertEquals(crit.getCondition(), Condition.EQUAL, "Condition should be EQUAL");
  }

  @Test
  public void testNewFilterWithCriterion() {
    // Create a Criterion manually and then build a filter from it.
    List<String> values = Arrays.asList("x", "y");
    Criterion crit =
        new Criterion()
            .setField("field4")
            .setValue("x")
            .setValues(new StringArray(values))
            .setCondition(Condition.EQUAL);
    Filter filter = QueryUtils.newFilter(crit);

    Assert.assertNotNull(filter.getOr(), "Filter 'or' clause should not be null");
    Assert.assertEquals(filter.getOr().size(), 1, "Expected one conjunctive criterion");
    ConjunctiveCriterion cc = filter.getOr().get(0);
    Assert.assertNotNull(cc.getAnd(), "ConjunctiveCriterion 'and' clause should not be null");
    Assert.assertEquals(cc.getAnd().size(), 1, "Expected one criterion in the filter");
    Criterion critFromFilter = cc.getAnd().get(0);
    Assert.assertEquals(critFromFilter.getField(), "field4", "Field name should match");
    Assert.assertEquals(critFromFilter.getValue(), "x", "Value should match");
    Assert.assertEquals(
        critFromFilter.getValues(),
        new StringArray(values),
        "Values should match the original list");
    Assert.assertEquals(
        critFromFilter.getCondition(), Condition.EQUAL, "Condition should be EQUAL");
  }
}
