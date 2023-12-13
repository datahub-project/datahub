package com.linkedin.metadata.search.utils;

import com.google.common.collect.ImmutableList;
import com.linkedin.data.template.StringArray;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.Criterion;
import org.opensearch.index.query.QueryBuilder;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ESUtilsTest {

  private static final String FIELD_TO_EXPAND = "fieldTags";

  @Test
  public void testGetQueryBuilderFromCriterionEqualsValues() {

    final Criterion singleValueCriterion =
        new Criterion()
            .setField("myTestField")
            .setCondition(Condition.EQUAL)
            .setValues(new StringArray(ImmutableList.of("value1")));

    QueryBuilder result = ESUtils.getQueryBuilderFromCriterion(singleValueCriterion, false);
    String expected =
        "{\n"
            + "  \"terms\" : {\n"
            + "    \"myTestField.keyword\" : [\n"
            + "      \"value1\"\n"
            + "    ],\n"
            + "    \"boost\" : 1.0,\n"
            + "    \"_name\" : \"myTestField\"\n"
            + "  }\n"
            + "}";
    Assert.assertEquals(result.toString(), expected);

    final Criterion multiValueCriterion =
        new Criterion()
            .setField("myTestField")
            .setCondition(Condition.EQUAL)
            .setValues(new StringArray(ImmutableList.of("value1", "value2")));

    result = ESUtils.getQueryBuilderFromCriterion(multiValueCriterion, false);
    expected =
        "{\n"
            + "  \"terms\" : {\n"
            + "    \"myTestField.keyword\" : [\n"
            + "      \"value1\",\n"
            + "      \"value2\"\n"
            + "    ],\n"
            + "    \"boost\" : 1.0,\n"
            + "    \"_name\" : \"myTestField\"\n"
            + "  }\n"
            + "}";
    Assert.assertEquals(result.toString(), expected);

    final Criterion timeseriesField =
        new Criterion()
            .setField("myTestField")
            .setCondition(Condition.EQUAL)
            .setValues(new StringArray(ImmutableList.of("value1", "value2")));

    result = ESUtils.getQueryBuilderFromCriterion(timeseriesField, true);
    expected =
        "{\n"
            + "  \"terms\" : {\n"
            + "    \"myTestField\" : [\n"
            + "      \"value1\",\n"
            + "      \"value2\"\n"
            + "    ],\n"
            + "    \"boost\" : 1.0,\n"
            + "    \"_name\" : \"myTestField\"\n"
            + "  }\n"
            + "}";
    Assert.assertEquals(result.toString(), expected);
  }

  @Test
  public void testGetQueryBuilderFromCriterionExists() {
    final Criterion singleValueCriterion =
        new Criterion().setField("myTestField").setCondition(Condition.EXISTS);

    QueryBuilder result = ESUtils.getQueryBuilderFromCriterion(singleValueCriterion, false);
    String expected =
        "{\n"
            + "  \"bool\" : {\n"
            + "    \"must\" : [\n"
            + "      {\n"
            + "        \"exists\" : {\n"
            + "          \"field\" : \"myTestField\",\n"
            + "          \"boost\" : 1.0\n"
            + "        }\n"
            + "      }\n"
            + "    ],\n"
            + "    \"adjust_pure_negative\" : true,\n"
            + "    \"boost\" : 1.0,\n"
            + "    \"_name\" : \"myTestField\"\n"
            + "  }\n"
            + "}";
    Assert.assertEquals(result.toString(), expected);

    // No diff in the timeseries field case for this condition.
    final Criterion timeseriesField =
        new Criterion().setField("myTestField").setCondition(Condition.EXISTS);

    result = ESUtils.getQueryBuilderFromCriterion(timeseriesField, true);
    expected =
        "{\n"
            + "  \"bool\" : {\n"
            + "    \"must\" : [\n"
            + "      {\n"
            + "        \"exists\" : {\n"
            + "          \"field\" : \"myTestField\",\n"
            + "          \"boost\" : 1.0\n"
            + "        }\n"
            + "      }\n"
            + "    ],\n"
            + "    \"adjust_pure_negative\" : true,\n"
            + "    \"boost\" : 1.0,\n"
            + "    \"_name\" : \"myTestField\"\n"
            + "  }\n"
            + "}";
    Assert.assertEquals(result.toString(), expected);
  }

  @Test
  public void testGetQueryBuilderFromCriterionIsNull() {
    final Criterion singleValueCriterion =
        new Criterion().setField("myTestField").setCondition(Condition.IS_NULL);

    QueryBuilder result = ESUtils.getQueryBuilderFromCriterion(singleValueCriterion, false);
    String expected =
        "{\n"
            + "  \"bool\" : {\n"
            + "    \"must_not\" : [\n"
            + "      {\n"
            + "        \"exists\" : {\n"
            + "          \"field\" : \"myTestField\",\n"
            + "          \"boost\" : 1.0\n"
            + "        }\n"
            + "      }\n"
            + "    ],\n"
            + "    \"adjust_pure_negative\" : true,\n"
            + "    \"boost\" : 1.0,\n"
            + "    \"_name\" : \"myTestField\"\n"
            + "  }\n"
            + "}";
    Assert.assertEquals(result.toString(), expected);

    // No diff in the timeseries case for this condition
    final Criterion timeseriesField =
        new Criterion().setField("myTestField").setCondition(Condition.IS_NULL);

    result = ESUtils.getQueryBuilderFromCriterion(timeseriesField, true);
    expected =
        "{\n"
            + "  \"bool\" : {\n"
            + "    \"must_not\" : [\n"
            + "      {\n"
            + "        \"exists\" : {\n"
            + "          \"field\" : \"myTestField\",\n"
            + "          \"boost\" : 1.0\n"
            + "        }\n"
            + "      }\n"
            + "    ],\n"
            + "    \"adjust_pure_negative\" : true,\n"
            + "    \"boost\" : 1.0,\n"
            + "    \"_name\" : \"myTestField\"\n"
            + "  }\n"
            + "}";
    Assert.assertEquals(result.toString(), expected);
  }

  @Test
  public void testGetQueryBuilderFromCriterionFieldToExpand() {

    final Criterion singleValueCriterion =
        new Criterion()
            .setField(FIELD_TO_EXPAND)
            .setCondition(Condition.EQUAL)
            .setValue("") // Ignored
            .setValues(new StringArray(ImmutableList.of("value1")));

    // Ensure that the query is expanded!
    QueryBuilder result = ESUtils.getQueryBuilderFromCriterion(singleValueCriterion, false);
    String expected =
        "{\n"
            + "  \"bool\" : {\n"
            + "    \"should\" : [\n"
            + "      {\n"
            + "        \"terms\" : {\n"
            + "          \"fieldTags.keyword\" : [\n"
            + "            \"value1\"\n"
            + "          ],\n"
            + "          \"boost\" : 1.0,\n"
            + "          \"_name\" : \"fieldTags\"\n"
            + "        }\n"
            + "      },\n"
            + "      {\n"
            + "        \"terms\" : {\n"
            + "          \"editedFieldTags.keyword\" : [\n"
            + "            \"value1\"\n"
            + "          ],\n"
            + "          \"boost\" : 1.0,\n"
            + "          \"_name\" : \"editedFieldTags\"\n"
            + "        }\n"
            + "      }\n"
            + "    ],\n"
            + "    \"adjust_pure_negative\" : true,\n"
            + "    \"boost\" : 1.0\n"
            + "  }\n"
            + "}";
    Assert.assertEquals(result.toString(), expected);

    final Criterion timeseriesField =
        new Criterion()
            .setField(FIELD_TO_EXPAND)
            .setCondition(Condition.EQUAL)
            .setValue("") // Ignored
            .setValues(new StringArray(ImmutableList.of("value1", "value2")));

    // Ensure that the query is expanded without keyword.
    result = ESUtils.getQueryBuilderFromCriterion(timeseriesField, true);
    expected =
        "{\n"
            + "  \"bool\" : {\n"
            + "    \"should\" : [\n"
            + "      {\n"
            + "        \"terms\" : {\n"
            + "          \"fieldTags\" : [\n"
            + "            \"value1\",\n"
            + "            \"value2\"\n"
            + "          ],\n"
            + "          \"boost\" : 1.0,\n"
            + "          \"_name\" : \"fieldTags\"\n"
            + "        }\n"
            + "      },\n"
            + "      {\n"
            + "        \"terms\" : {\n"
            + "          \"editedFieldTags\" : [\n"
            + "            \"value1\",\n"
            + "            \"value2\"\n"
            + "          ],\n"
            + "          \"boost\" : 1.0,\n"
            + "          \"_name\" : \"editedFieldTags\"\n"
            + "        }\n"
            + "      }\n"
            + "    ],\n"
            + "    \"adjust_pure_negative\" : true,\n"
            + "    \"boost\" : 1.0\n"
            + "  }\n"
            + "}";
    Assert.assertEquals(result.toString(), expected);
  }
}
