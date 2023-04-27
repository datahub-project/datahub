package com.linkedin.datahub.graphql.resolvers.search;

import com.google.common.collect.ImmutableList;
import com.linkedin.data.template.StringArray;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.Test;

public class SearchUtilsTest {

  @Test
  public static void testApplyViewToFilterNullBaseFilter() {

    Filter viewFilter = new Filter()
      .setOr(new ConjunctiveCriterionArray(
          new ConjunctiveCriterion().setAnd(
              new CriterionArray(ImmutableList.of(
                  new Criterion()
                      .setField("field")
                      .setValue("test")
                      .setValues(new StringArray(ImmutableList.of("test")))
              ))
          )));

    Filter result = SearchUtils.combineFilters(null, viewFilter);
    Assert.assertEquals(viewFilter, result);
  }

  @Test
  public static void testApplyViewToFilterComplexBaseFilter() {
    Filter baseFilter = new Filter()
      .setOr(new ConjunctiveCriterionArray(
          ImmutableList.of(
              new ConjunctiveCriterion().setAnd(
                  new CriterionArray(ImmutableList.of(
                      new Criterion()
                          .setField("field1")
                          .setValue("test1")
                          .setValues(new StringArray(ImmutableList.of("test1"))),
                      new Criterion()
                          .setField("field2")
                          .setValue("test2")
                          .setValues(new StringArray(ImmutableList.of("test2")))
                  ))
              ),
              new ConjunctiveCriterion().setAnd(
                  new CriterionArray(ImmutableList.of(
                      new Criterion()
                          .setField("field3")
                          .setValue("test3")
                          .setValues(new StringArray(ImmutableList.of("test3"))),
                      new Criterion()
                          .setField("field4")
                          .setValue("test4")
                          .setValues(new StringArray(ImmutableList.of("test4")))
                  ))
              )
          )));

    Filter viewFilter = new Filter()
        .setOr(new ConjunctiveCriterionArray(
            new ConjunctiveCriterion().setAnd(
                new CriterionArray(ImmutableList.of(
                    new Criterion()
                        .setField("field")
                        .setValue("test")
                        .setValues(new StringArray(ImmutableList.of("test")))
                ))
            )));

    Filter result = SearchUtils.combineFilters(baseFilter, viewFilter);

    Filter expectedResult = new Filter()
        .setOr(new ConjunctiveCriterionArray(
            ImmutableList.of(
                new ConjunctiveCriterion().setAnd(
                    new CriterionArray(ImmutableList.of(
                        new Criterion()
                            .setField("field1")
                            .setValue("test1")
                            .setValues(new StringArray(ImmutableList.of("test1"))),
                        new Criterion()
                            .setField("field2")
                            .setValue("test2")
                            .setValues(new StringArray(ImmutableList.of("test2"))),
                        new Criterion()
                            .setField("field")
                            .setValue("test")
                            .setValues(new StringArray(ImmutableList.of("test")))
                    ))
                ),
                new ConjunctiveCriterion().setAnd(
                    new CriterionArray(ImmutableList.of(
                        new Criterion()
                            .setField("field3")
                            .setValue("test3")
                            .setValues(new StringArray(ImmutableList.of("test3"))),
                        new Criterion()
                            .setField("field4")
                            .setValue("test4")
                            .setValues(new StringArray(ImmutableList.of("test4"))),
                        new Criterion()
                            .setField("field")
                            .setValue("test")
                            .setValues(new StringArray(ImmutableList.of("test")))
                    ))
                )
            )));

    Assert.assertEquals(expectedResult, result);
  }

  @Test
  public static void testApplyViewToFilterComplexViewFilter() {
    Filter baseFilter = new Filter()
        .setOr(new ConjunctiveCriterionArray(
            ImmutableList.of(
                new ConjunctiveCriterion().setAnd(
                    new CriterionArray(ImmutableList.of(
                        new Criterion()
                            .setField("field1")
                            .setValue("test1")
                            .setValues(new StringArray(ImmutableList.of("test1"))),
                        new Criterion()
                            .setField("field2")
                            .setValue("test2")
                            .setValues(new StringArray(ImmutableList.of("test2")))
                    ))
                ),
                new ConjunctiveCriterion().setAnd(
                    new CriterionArray(ImmutableList.of(
                        new Criterion()
                            .setField("field3")
                            .setValue("test3")
                            .setValues(new StringArray(ImmutableList.of("test3"))),
                        new Criterion()
                            .setField("field4")
                            .setValue("test4")
                            .setValues(new StringArray(ImmutableList.of("test4")))
                    ))
                )
            )));

    Filter viewFilter = new Filter()
        .setOr(new ConjunctiveCriterionArray(
            ImmutableList.of(
                new ConjunctiveCriterion().setAnd(
                    new CriterionArray(ImmutableList.of(
                        new Criterion()
                            .setField("viewField1")
                            .setValue("viewTest1")
                            .setValues(new StringArray(ImmutableList.of("viewTest1"))),
                        new Criterion()
                            .setField("viewField2")
                            .setValue("viewTest2")
                            .setValues(new StringArray(ImmutableList.of("viewTest2")))
                    ))
                ),
                new ConjunctiveCriterion().setAnd(
                    new CriterionArray(ImmutableList.of(
                        new Criterion()
                            .setField("viewField3")
                            .setValue("viewTest3")
                            .setValues(new StringArray(ImmutableList.of("viewTest3"))),
                        new Criterion()
                            .setField("viewField4")
                            .setValue("viewTest4")
                            .setValues(new StringArray(ImmutableList.of("viewTest4")))
                    ))
                )
            )));

    Filter result = SearchUtils.combineFilters(baseFilter, viewFilter);

    Filter expectedResult = new Filter()
        .setOr(new ConjunctiveCriterionArray(
            ImmutableList.of(
                new ConjunctiveCriterion().setAnd(
                    new CriterionArray(ImmutableList.of(
                        new Criterion()
                            .setField("field1")
                            .setValue("test1")
                            .setValues(new StringArray(ImmutableList.of("test1"))),
                        new Criterion()
                            .setField("field2")
                            .setValue("test2")
                            .setValues(new StringArray(ImmutableList.of("test2"))),
                        new Criterion()
                            .setField("viewField1")
                            .setValue("viewTest1")
                            .setValues(new StringArray(ImmutableList.of("viewTest1"))),
                        new Criterion()
                            .setField("viewField2")
                            .setValue("viewTest2")
                            .setValues(new StringArray(ImmutableList.of("viewTest2")))
                    ))
                ),
                new ConjunctiveCriterion().setAnd(
                    new CriterionArray(ImmutableList.of(
                        new Criterion()
                            .setField("field1")
                            .setValue("test1")
                            .setValues(new StringArray(ImmutableList.of("test1"))),
                        new Criterion()
                            .setField("field2")
                            .setValue("test2")
                            .setValues(new StringArray(ImmutableList.of("test2"))),
                        new Criterion()
                            .setField("viewField3")
                            .setValue("viewTest3")
                            .setValues(new StringArray(ImmutableList.of("viewTest3"))),
                        new Criterion()
                            .setField("viewField4")
                            .setValue("viewTest4")
                            .setValues(new StringArray(ImmutableList.of("viewTest4")))
                    ))
                ),
                new ConjunctiveCriterion().setAnd(
                    new CriterionArray(ImmutableList.of(
                        new Criterion()
                            .setField("field3")
                            .setValue("test3")
                            .setValues(new StringArray(ImmutableList.of("test3"))),
                        new Criterion()
                            .setField("field4")
                            .setValue("test4")
                            .setValues(new StringArray(ImmutableList.of("test4"))),
                        new Criterion()
                            .setField("viewField1")
                            .setValue("viewTest1")
                            .setValues(new StringArray(ImmutableList.of("viewTest1"))),
                        new Criterion()
                            .setField("viewField2")
                            .setValue("viewTest2")
                            .setValues(new StringArray(ImmutableList.of("viewTest2")))
                    ))
                ),
                new ConjunctiveCriterion().setAnd(
                    new CriterionArray(ImmutableList.of(
                        new Criterion()
                            .setField("field3")
                            .setValue("test3")
                            .setValues(new StringArray(ImmutableList.of("test3"))),
                        new Criterion()
                            .setField("field4")
                            .setValue("test4")
                            .setValues(new StringArray(ImmutableList.of("test4"))),
                        new Criterion()
                            .setField("viewField3")
                            .setValue("viewTest3")
                            .setValues(new StringArray(ImmutableList.of("viewTest3"))),
                        new Criterion()
                            .setField("viewField4")
                            .setValue("viewTest4")
                            .setValues(new StringArray(ImmutableList.of("viewTest4")))
                    ))
                )
            )));

    Assert.assertEquals(expectedResult, result);
  }

  @Test
  public static void testApplyViewToFilterV1Filter() {
    Filter baseFilter = new Filter()
        .setCriteria(
          new CriterionArray(ImmutableList.of(
              new Criterion()
                  .setField("field1")
                  .setValue("test1")
                  .setValues(new StringArray(ImmutableList.of("test1"))),
              new Criterion()
                  .setField("field2")
                  .setValue("test2")
                  .setValues(new StringArray(ImmutableList.of("test2")))
          ))
        );

    Filter viewFilter = new Filter()
        .setCriteria(
            new CriterionArray(ImmutableList.of(
                new Criterion()
                    .setField("viewField1")
                    .setValue("viewTest1")
                    .setValues(new StringArray(ImmutableList.of("viewTest1"))),
                new Criterion()
                    .setField("viewField2")
                    .setValue("viewTest2")
                    .setValues(new StringArray(ImmutableList.of("viewTest2")))
            ))
        );

    Filter result = SearchUtils.combineFilters(baseFilter, viewFilter);

    Filter expectedResult = new Filter()
        .setOr(new ConjunctiveCriterionArray(
            ImmutableList.of(
                new ConjunctiveCriterion().setAnd(
                    new CriterionArray(ImmutableList.of(
                        new Criterion()
                            .setField("field1")
                            .setValue("test1")
                            .setValues(new StringArray(ImmutableList.of("test1"))),
                        new Criterion()
                            .setField("field2")
                            .setValue("test2")
                            .setValues(new StringArray(ImmutableList.of("test2"))),
                        new Criterion()
                            .setField("viewField1")
                            .setValue("viewTest1")
                            .setValues(new StringArray(ImmutableList.of("viewTest1"))),
                        new Criterion()
                            .setField("viewField2")
                            .setValue("viewTest2")
                            .setValues(new StringArray(ImmutableList.of("viewTest2")))
                    ))
                )
            )));

    Assert.assertEquals(expectedResult, result);
  }

  @Test
  public static void testApplyViewToEntityTypes() {

    List<String> baseEntityTypes = ImmutableList.of(
        Constants.CHART_ENTITY_NAME,
        Constants.DATASET_ENTITY_NAME
    );

    List<String> viewEntityTypes = ImmutableList.of(
        Constants.DATASET_ENTITY_NAME,
        Constants.DASHBOARD_ENTITY_NAME
    );

    final List<String> result = SearchUtils.intersectEntityTypes(baseEntityTypes, viewEntityTypes);

    final List<String> expectedResult = ImmutableList.of(
        Constants.DATASET_ENTITY_NAME
    );
    Assert.assertEquals(expectedResult, result);
  }

  private SearchUtilsTest() { }

}
