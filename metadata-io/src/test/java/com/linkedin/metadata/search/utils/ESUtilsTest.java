package com.linkedin.metadata.search.utils;

import static com.linkedin.metadata.Constants.DATA_TYPE_URN_PREFIX;
import static com.linkedin.metadata.Constants.STRUCTURED_PROPERTY_DEFINITION_ASPECT_NAME;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.SetMode;
import com.linkedin.data.template.StringArray;
import com.linkedin.entity.Aspect;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.structured.StructuredPropertyDefinition;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.opensearch.index.query.QueryBuilder;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class ESUtilsTest {

  private static final String FIELD_TO_EXPAND = "fieldTags";

  private static AspectRetriever aspectRetriever;
  private static AspectRetriever aspectRetrieverV1;

  @BeforeClass
  public static void setup() throws RemoteInvocationException, URISyntaxException {
    Urn abFghTenUrn = Urn.createFromString("urn:li:structuredProperty:ab.fgh.ten");

    // legacy
    aspectRetriever = mock(AspectRetriever.class);
    when(aspectRetriever.getEntityRegistry())
        .thenReturn(TestOperationContexts.defaultEntityRegistry());

    StructuredPropertyDefinition structPropAbFghTenDefinition = new StructuredPropertyDefinition();
    structPropAbFghTenDefinition.setVersion(null, SetMode.REMOVE_IF_NULL);
    structPropAbFghTenDefinition.setValueType(
        Urn.createFromString(DATA_TYPE_URN_PREFIX + "string"));
    structPropAbFghTenDefinition.setQualifiedName("ab.fgh.ten");
    when(aspectRetriever.getLatestAspectObjects(eq(Set.of(abFghTenUrn)), anySet()))
        .thenReturn(
            Map.of(
                abFghTenUrn,
                Map.of(
                    STRUCTURED_PROPERTY_DEFINITION_ASPECT_NAME,
                    new Aspect(structPropAbFghTenDefinition.data()))));

    // V1
    aspectRetrieverV1 = mock(AspectRetriever.class);
    when(aspectRetrieverV1.getEntityRegistry())
        .thenReturn(TestOperationContexts.defaultEntityRegistry());

    StructuredPropertyDefinition structPropAbFghTenDefinitionV1 =
        new StructuredPropertyDefinition();
    structPropAbFghTenDefinitionV1.setVersion("00000000000001");
    structPropAbFghTenDefinitionV1.setValueType(
        Urn.createFromString(DATA_TYPE_URN_PREFIX + "string"));
    structPropAbFghTenDefinitionV1.setQualifiedName("ab.fgh.ten");
    when(aspectRetrieverV1.getLatestAspectObjects(eq(Set.of(abFghTenUrn)), anySet()))
        .thenReturn(
            Map.of(
                abFghTenUrn,
                Map.of(
                    STRUCTURED_PROPERTY_DEFINITION_ASPECT_NAME,
                    new Aspect(structPropAbFghTenDefinitionV1.data()))));
  }

  @Test
  public void testGetQueryBuilderFromCriterionEqualsValues() {

    final Criterion singleValueCriterion =
        new Criterion()
            .setField("myTestField")
            .setCondition(Condition.EQUAL)
            .setValues(new StringArray(ImmutableList.of("value1")));

    QueryBuilder result =
        ESUtils.getQueryBuilderFromCriterion(
            singleValueCriterion, false, new HashMap<>(), mock(AspectRetriever.class));
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

    result =
        ESUtils.getQueryBuilderFromCriterion(
            multiValueCriterion, false, new HashMap<>(), mock(AspectRetriever.class));
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

    result =
        ESUtils.getQueryBuilderFromCriterion(
            timeseriesField, true, new HashMap<>(), mock(AspectRetriever.class));
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

    QueryBuilder result =
        ESUtils.getQueryBuilderFromCriterion(
            singleValueCriterion, false, new HashMap<>(), mock(AspectRetriever.class));
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

    result =
        ESUtils.getQueryBuilderFromCriterion(
            timeseriesField, true, new HashMap<>(), mock(AspectRetriever.class));
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

    QueryBuilder result =
        ESUtils.getQueryBuilderFromCriterion(
            singleValueCriterion, false, new HashMap<>(), mock(AspectRetriever.class));
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

    result =
        ESUtils.getQueryBuilderFromCriterion(
            timeseriesField, true, new HashMap<>(), mock(AspectRetriever.class));
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
    QueryBuilder result =
        ESUtils.getQueryBuilderFromCriterion(
            singleValueCriterion, false, new HashMap<>(), mock(AspectRetriever.class));
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
    result =
        ESUtils.getQueryBuilderFromCriterion(
            timeseriesField, true, new HashMap<>(), mock(AspectRetriever.class));
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

  @Test
  public void testGetQueryBuilderFromStructPropEqualsValue() {

    final Criterion singleValueCriterion =
        new Criterion()
            .setField("structuredProperties.ab.fgh.ten")
            .setCondition(Condition.EQUAL)
            .setValues(new StringArray(ImmutableList.of("value1")));

    QueryBuilder result =
        ESUtils.getQueryBuilderFromCriterion(
            singleValueCriterion, false, new HashMap<>(), aspectRetriever);
    String expected =
        "{\n"
            + "  \"terms\" : {\n"
            + "    \"structuredProperties.ab_fgh_ten.keyword\" : [\n"
            + "      \"value1\"\n"
            + "    ],\n"
            + "    \"boost\" : 1.0,\n"
            + "    \"_name\" : \"structuredProperties.ab.fgh.ten\"\n"
            + "  }\n"
            + "}";
    Assert.assertEquals(result.toString(), expected);
  }

  @Test
  public void testGetQueryBuilderFromStructPropEqualsValueV1() {

    final Criterion singleValueCriterion =
        new Criterion()
            .setField("structuredProperties.ab.fgh.ten")
            .setCondition(Condition.EQUAL)
            .setValues(new StringArray(ImmutableList.of("value1")));

    QueryBuilder result =
        ESUtils.getQueryBuilderFromCriterion(
            singleValueCriterion, false, new HashMap<>(), aspectRetrieverV1);
    String expected =
        "{\n"
            + "  \"terms\" : {\n"
            + "    \"structuredProperties._versioned.ab_fgh_ten.00000000000001.string.keyword\" : [\n"
            + "      \"value1\"\n"
            + "    ],\n"
            + "    \"boost\" : 1.0,\n"
            + "    \"_name\" : \"structuredProperties.ab.fgh.ten\"\n"
            + "  }\n"
            + "}";
    Assert.assertEquals(result.toString(), expected);
  }

  @Test
  public void testGetQueryBuilderFromStructPropExists() {
    final Criterion singleValueCriterion =
        new Criterion().setField("structuredProperties.ab.fgh.ten").setCondition(Condition.EXISTS);

    QueryBuilder result =
        ESUtils.getQueryBuilderFromCriterion(
            singleValueCriterion, false, new HashMap<>(), aspectRetriever);
    String expected =
        "{\n"
            + "  \"bool\" : {\n"
            + "    \"must\" : [\n"
            + "      {\n"
            + "        \"exists\" : {\n"
            + "          \"field\" : \"structuredProperties.ab_fgh_ten\",\n"
            + "          \"boost\" : 1.0\n"
            + "        }\n"
            + "      }\n"
            + "    ],\n"
            + "    \"adjust_pure_negative\" : true,\n"
            + "    \"boost\" : 1.0,\n"
            + "    \"_name\" : \"structuredProperties.ab.fgh.ten\"\n"
            + "  }\n"
            + "}";
    Assert.assertEquals(result.toString(), expected);

    // No diff in the timeseries field case for this condition.
    final Criterion timeseriesField =
        new Criterion().setField("myTestField").setCondition(Condition.EXISTS);

    result =
        ESUtils.getQueryBuilderFromCriterion(
            timeseriesField, true, new HashMap<>(), aspectRetriever);
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
  public void testGetQueryBuilderFromStructPropExistsV1() {
    final Criterion singleValueCriterion =
        new Criterion().setField("structuredProperties.ab.fgh.ten").setCondition(Condition.EXISTS);

    QueryBuilder result =
        ESUtils.getQueryBuilderFromCriterion(
            singleValueCriterion, false, new HashMap<>(), aspectRetrieverV1);
    String expected =
        "{\n"
            + "  \"bool\" : {\n"
            + "    \"must\" : [\n"
            + "      {\n"
            + "        \"exists\" : {\n"
            + "          \"field\" : \"structuredProperties._versioned.ab_fgh_ten.00000000000001.string\",\n"
            + "          \"boost\" : 1.0\n"
            + "        }\n"
            + "      }\n"
            + "    ],\n"
            + "    \"adjust_pure_negative\" : true,\n"
            + "    \"boost\" : 1.0,\n"
            + "    \"_name\" : \"structuredProperties.ab.fgh.ten\"\n"
            + "  }\n"
            + "}";
    Assert.assertEquals(result.toString(), expected);

    // No diff in the timeseries field case for this condition.
    final Criterion timeseriesField =
        new Criterion().setField("myTestField").setCondition(Condition.EXISTS);

    result =
        ESUtils.getQueryBuilderFromCriterion(
            timeseriesField, true, new HashMap<>(), aspectRetrieverV1);
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
}
