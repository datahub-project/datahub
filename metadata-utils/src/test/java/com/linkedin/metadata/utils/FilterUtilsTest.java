package com.linkedin.metadata.utils;

import static com.linkedin.metadata.utils.CriterionUtils.buildCriterion;

import com.google.common.collect.ImmutableList;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.utils.elasticsearch.FilterUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

public class FilterUtilsTest {

  @Test
  public static void testApplyViewToFilterNullBaseFilter() {

    Filter viewFilter =
        new Filter()
            .setOr(
                new ConjunctiveCriterionArray(
                    new ConjunctiveCriterion()
                        .setAnd(
                            new CriterionArray(
                                ImmutableList.of(
                                    buildCriterion("field", Condition.EQUAL, "test"))))));

    Filter result = FilterUtils.combineFilters(null, viewFilter);
    Assert.assertEquals(viewFilter, result);
  }

  @Test
  public static void testApplyViewToFilterComplexBaseFilter() {
    Filter baseFilter =
        new Filter()
            .setOr(
                new ConjunctiveCriterionArray(
                    ImmutableList.of(
                        new ConjunctiveCriterion()
                            .setAnd(
                                new CriterionArray(
                                    ImmutableList.of(
                                        buildCriterion("field1", Condition.EQUAL, "test1"),
                                        buildCriterion("field2", Condition.EQUAL, "test2")))),
                        new ConjunctiveCriterion()
                            .setAnd(
                                new CriterionArray(
                                    ImmutableList.of(
                                        buildCriterion("field3", Condition.EQUAL, "test3"),
                                        buildCriterion("field4", Condition.EQUAL, "test4")))))));

    Filter viewFilter =
        new Filter()
            .setOr(
                new ConjunctiveCriterionArray(
                    new ConjunctiveCriterion()
                        .setAnd(
                            new CriterionArray(
                                ImmutableList.of(
                                    buildCriterion("field", Condition.EQUAL, "test"))))));

    Filter result = FilterUtils.combineFilters(baseFilter, viewFilter);

    Filter expectedResult =
        new Filter()
            .setOr(
                new ConjunctiveCriterionArray(
                    ImmutableList.of(
                        new ConjunctiveCriterion()
                            .setAnd(
                                new CriterionArray(
                                    ImmutableList.of(
                                        buildCriterion("field1", Condition.EQUAL, "test1"),
                                        buildCriterion("field2", Condition.EQUAL, "test2"),
                                        buildCriterion("field", Condition.EQUAL, "test")))),
                        new ConjunctiveCriterion()
                            .setAnd(
                                new CriterionArray(
                                    ImmutableList.of(
                                        buildCriterion("field3", Condition.EQUAL, "test3"),
                                        buildCriterion("field4", Condition.EQUAL, "test4"),
                                        buildCriterion("field", Condition.EQUAL, "test")))))));

    Assert.assertEquals(expectedResult, result);
  }

  @Test
  public static void testApplyViewToFilterComplexViewFilter() {
    Filter baseFilter =
        new Filter()
            .setOr(
                new ConjunctiveCriterionArray(
                    ImmutableList.of(
                        new ConjunctiveCriterion()
                            .setAnd(
                                new CriterionArray(
                                    ImmutableList.of(
                                        buildCriterion("field1", Condition.EQUAL, "test1"),
                                        buildCriterion("field2", Condition.EQUAL, "test2")))),
                        new ConjunctiveCriterion()
                            .setAnd(
                                new CriterionArray(
                                    ImmutableList.of(
                                        buildCriterion("field3", Condition.EQUAL, "test3"),
                                        buildCriterion("field4", Condition.EQUAL, "test4")))))));

    Filter viewFilter =
        new Filter()
            .setOr(
                new ConjunctiveCriterionArray(
                    ImmutableList.of(
                        new ConjunctiveCriterion()
                            .setAnd(
                                new CriterionArray(
                                    ImmutableList.of(
                                        buildCriterion("viewField1", Condition.EQUAL, "viewTest1"),
                                        buildCriterion(
                                            "viewField2", Condition.EQUAL, "viewTest2")))),
                        new ConjunctiveCriterion()
                            .setAnd(
                                new CriterionArray(
                                    ImmutableList.of(
                                        buildCriterion("viewField3", Condition.EQUAL, "viewTest3"),
                                        buildCriterion(
                                            "viewField4", Condition.EQUAL, "viewTest4")))))));

    Filter result = FilterUtils.combineFilters(baseFilter, viewFilter);

    Filter expectedResult =
        new Filter()
            .setOr(
                new ConjunctiveCriterionArray(
                    ImmutableList.of(
                        new ConjunctiveCriterion()
                            .setAnd(
                                new CriterionArray(
                                    ImmutableList.of(
                                        buildCriterion("field1", Condition.EQUAL, "test1"),
                                        buildCriterion("field2", Condition.EQUAL, "test2"),
                                        buildCriterion("viewField1", Condition.EQUAL, "viewTest1"),
                                        buildCriterion(
                                            "viewField2", Condition.EQUAL, "viewTest2")))),
                        new ConjunctiveCriterion()
                            .setAnd(
                                new CriterionArray(
                                    ImmutableList.of(
                                        buildCriterion("field1", Condition.EQUAL, "test1"),
                                        buildCriterion("field2", Condition.EQUAL, "test2"),
                                        buildCriterion("viewField3", Condition.EQUAL, "viewTest3"),
                                        buildCriterion(
                                            "viewField4", Condition.EQUAL, "viewTest4")))),
                        new ConjunctiveCriterion()
                            .setAnd(
                                new CriterionArray(
                                    ImmutableList.of(
                                        buildCriterion("field3", Condition.EQUAL, "test3"),
                                        buildCriterion("field4", Condition.EQUAL, "test4"),
                                        buildCriterion("viewField1", Condition.EQUAL, "viewTest1"),
                                        buildCriterion(
                                            "viewField2", Condition.EQUAL, "viewTest2")))),
                        new ConjunctiveCriterion()
                            .setAnd(
                                new CriterionArray(
                                    ImmutableList.of(
                                        buildCriterion("field3", Condition.EQUAL, "test3"),
                                        buildCriterion("field4", Condition.EQUAL, "test4"),
                                        buildCriterion("viewField3", Condition.EQUAL, "viewTest3"),
                                        buildCriterion(
                                            "viewField4", Condition.EQUAL, "viewTest4")))))));

    Assert.assertEquals(expectedResult, result);
  }

  @Test
  public static void testApplyViewToFilterV1Filter() {
    Filter baseFilter =
        new Filter()
            .setCriteria(
                new CriterionArray(
                    ImmutableList.of(
                        buildCriterion("field1", Condition.EQUAL, "test1"),
                        buildCriterion("field2", Condition.EQUAL, "test2"))));

    Filter viewFilter =
        new Filter()
            .setCriteria(
                new CriterionArray(
                    ImmutableList.of(
                        buildCriterion("viewField1", Condition.EQUAL, "viewTest1"),
                        buildCriterion("viewField2", Condition.EQUAL, "viewTest2"))));

    Filter result = FilterUtils.combineFilters(baseFilter, viewFilter);

    Filter expectedResult =
        new Filter()
            .setOr(
                new ConjunctiveCriterionArray(
                    ImmutableList.of(
                        new ConjunctiveCriterion()
                            .setAnd(
                                new CriterionArray(
                                    ImmutableList.of(
                                        buildCriterion("field1", Condition.EQUAL, "test1"),
                                        buildCriterion("field2", Condition.EQUAL, "test2"),
                                        buildCriterion("viewField1", Condition.EQUAL, "viewTest1"),
                                        buildCriterion(
                                            "viewField2", Condition.EQUAL, "viewTest2")))))));

    Assert.assertEquals(expectedResult, result);
  }
}
