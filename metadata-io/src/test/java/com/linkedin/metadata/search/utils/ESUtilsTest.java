package com.linkedin.metadata.search.utils;

import static com.linkedin.metadata.Constants.DATA_TYPE_URN_PREFIX;
import static com.linkedin.metadata.Constants.STRUCTURED_PROPERTY_DEFINITION_ASPECT_NAME;
import static com.linkedin.metadata.utils.CriterionUtils.buildCriterion;
import static com.linkedin.metadata.utils.CriterionUtils.buildExistsCriterion;
import static com.linkedin.metadata.utils.CriterionUtils.buildIsNullCriterion;
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
import com.linkedin.metadata.models.annotation.SearchableAnnotation;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.search.elasticsearch.query.filter.QueryFilterRewriteChain;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.structured.StructuredPropertyDefinition;
import io.datahubproject.metadata.context.OperationContext;
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
    Urn underscoresAndDotsUrn =
        Urn.createFromString("urn:li:structuredProperty:under.scores.and.dots_make_a_mess");
    Urn dateWithDotsUrn = Urn.createFromString("urn:li:structuredProperty:date_here.with_dot");

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

    StructuredPropertyDefinition dateWithDotsDefinition = new StructuredPropertyDefinition();
    dateWithDotsDefinition.setVersion(null, SetMode.REMOVE_IF_NULL);
    dateWithDotsDefinition.setValueType(Urn.createFromString(DATA_TYPE_URN_PREFIX + "date"));
    dateWithDotsDefinition.setQualifiedName("date_here.with_dot");
    when(aspectRetriever.getLatestAspectObjects(eq(Set.of(dateWithDotsUrn)), anySet()))
        .thenReturn(
            Map.of(
                dateWithDotsUrn,
                Map.of(
                    STRUCTURED_PROPERTY_DEFINITION_ASPECT_NAME,
                    new Aspect(dateWithDotsDefinition.data()))));

    StructuredPropertyDefinition structPropUnderscoresAndDotsDefinition =
        new StructuredPropertyDefinition();
    structPropUnderscoresAndDotsDefinition.setVersion(null, SetMode.REMOVE_IF_NULL);
    structPropUnderscoresAndDotsDefinition.setValueType(
        Urn.createFromString(DATA_TYPE_URN_PREFIX + "string"));
    structPropUnderscoresAndDotsDefinition.setQualifiedName("under.scores.and.dots_make_a_mess");
    when(aspectRetriever.getLatestAspectObjects(eq(Set.of(underscoresAndDotsUrn)), anySet()))
        .thenReturn(
            Map.of(
                underscoresAndDotsUrn,
                Map.of(
                    STRUCTURED_PROPERTY_DEFINITION_ASPECT_NAME,
                    new Aspect(structPropUnderscoresAndDotsDefinition.data()))));

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

    StructuredPropertyDefinition structPropUnderscoresAndDotsDefinitionV1 =
        new StructuredPropertyDefinition();
    structPropUnderscoresAndDotsDefinitionV1.setVersion("00000000000001");
    structPropUnderscoresAndDotsDefinitionV1.setValueType(
        Urn.createFromString(DATA_TYPE_URN_PREFIX + "string"));
    structPropUnderscoresAndDotsDefinitionV1.setQualifiedName("under.scores.and.dots_make_a_mess");
    when(aspectRetrieverV1.getLatestAspectObjects(eq(Set.of(underscoresAndDotsUrn)), anySet()))
        .thenReturn(
            Map.of(
                underscoresAndDotsUrn,
                Map.of(
                    STRUCTURED_PROPERTY_DEFINITION_ASPECT_NAME,
                    new Aspect(structPropUnderscoresAndDotsDefinitionV1.data()))));
  }

  @Test
  public void testGetQueryBuilderFromCriterionEqualsValues() {

    final Criterion singleValueCriterion = buildCriterion("myTestField", Condition.EQUAL, "value1");

    QueryBuilder result =
        ESUtils.getQueryBuilderFromCriterion(
            singleValueCriterion,
            false,
            new HashMap<>(),
            mock(OperationContext.class),
            QueryFilterRewriteChain.EMPTY);
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
        buildCriterion("myTestField", Condition.EQUAL, "value1", "value2");

    result =
        ESUtils.getQueryBuilderFromCriterion(
            multiValueCriterion,
            false,
            new HashMap<>(),
            mock(OperationContext.class),
            QueryFilterRewriteChain.EMPTY);
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
        buildCriterion("myTestField", Condition.EQUAL, "value1", "value2");

    result =
        ESUtils.getQueryBuilderFromCriterion(
            timeseriesField,
            true,
            new HashMap<>(),
            mock(OperationContext.class),
            QueryFilterRewriteChain.EMPTY);
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
  public void testGetQueryBuilderFromCriterionIEqualValues() { // Test case insensitive searches

    final Criterion singleValueCriterion =
        buildCriterion("myTestField", Condition.IEQUAL, "value1");

    QueryBuilder result =
        ESUtils.getQueryBuilderFromCriterion(
            singleValueCriterion,
            false,
            new HashMap<>(),
            mock(OperationContext.class),
            QueryFilterRewriteChain.EMPTY);

    String expected =
        "{\n"
            + "  \"bool\" : {\n"
            + "    \"should\" : [\n"
            + "      {\n"
            + "        \"term\" : {\n"
            + "          \"myTestField.keyword\" : {\n"
            + "            \"value\" : \"value1\",\n"
            + "            \"case_insensitive\" : true,\n"
            + "            \"boost\" : 1.0\n"
            + "          }\n"
            + "        }\n"
            + "      }\n"
            + "    ],\n"
            + "    \"adjust_pure_negative\" : true,\n"
            + "    \"boost\" : 1.0,\n"
            + "    \"_name\" : \"myTestField\"\n"
            + "  }\n"
            + "}";

    Assert.assertEquals(result.toString(), expected);

    final Criterion multiValueCriterion =
        buildCriterion("myTestField", Condition.IEQUAL, "value1", "value2");

    result =
        ESUtils.getQueryBuilderFromCriterion(
            multiValueCriterion,
            false,
            new HashMap<>(),
            mock(OperationContext.class),
            QueryFilterRewriteChain.EMPTY);

    expected =
        "{\n"
            + "  \"bool\" : {\n"
            + "    \"should\" : [\n"
            + "      {\n"
            + "        \"term\" : {\n"
            + "          \"myTestField.keyword\" : {\n"
            + "            \"value\" : \"value1\",\n"
            + "            \"case_insensitive\" : true,\n"
            + "            \"boost\" : 1.0\n"
            + "          }\n"
            + "        }\n"
            + "      },\n"
            + "      {\n"
            + "        \"term\" : {\n"
            + "          \"myTestField.keyword\" : {\n"
            + "            \"value\" : \"value2\",\n"
            + "            \"case_insensitive\" : true,\n"
            + "            \"boost\" : 1.0\n"
            + "          }\n"
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
  public void testGetQueryBuilderFromCriterionContain() {
    final Criterion singleValueCriterion =
        buildCriterion("myTestField", Condition.CONTAIN, "value1");

    QueryBuilder result =
        ESUtils.getQueryBuilderFromCriterion(
            singleValueCriterion,
            false,
            new HashMap<>(),
            mock(OperationContext.class),
            mock(QueryFilterRewriteChain.class));

    String expected =
        "{\n"
            + "  \"bool\" : {\n"
            + "    \"should\" : [\n"
            + "      {\n"
            + "        \"wildcard\" : {\n"
            + "          \"myTestField.keyword\" : {\n"
            + "            \"wildcard\" : \"*value1*\",\n"
            + "            \"case_insensitive\" : true,\n"
            + "            \"boost\" : 1.0,\n"
            + "            \"_name\" : \"myTestField\"\n"
            + "          }\n"
            + "        }\n"
            + "      }\n"
            + "    ],\n"
            + "    \"adjust_pure_negative\" : true,\n"
            + "    \"minimum_should_match\" : \"1\",\n"
            + "    \"boost\" : 1.0\n"
            + "  }\n"
            + "}";

    Assert.assertEquals(result.toString(), expected);

    final Criterion multiValueCriterion =
        buildCriterion("myTestField", Condition.CONTAIN, "value1", "value2");

    result =
        ESUtils.getQueryBuilderFromCriterion(
            multiValueCriterion,
            false,
            new HashMap<>(),
            mock(OperationContext.class),
            mock(QueryFilterRewriteChain.class));

    expected =
        "{\n"
            + "  \"bool\" : {\n"
            + "    \"should\" : [\n"
            + "      {\n"
            + "        \"wildcard\" : {\n"
            + "          \"myTestField.keyword\" : {\n"
            + "            \"wildcard\" : \"*value1*\",\n"
            + "            \"case_insensitive\" : true,\n"
            + "            \"boost\" : 1.0,\n"
            + "            \"_name\" : \"myTestField\"\n"
            + "          }\n"
            + "        }\n"
            + "      },\n"
            + "      {\n"
            + "        \"wildcard\" : {\n"
            + "          \"myTestField.keyword\" : {\n"
            + "            \"wildcard\" : \"*value2*\",\n"
            + "            \"case_insensitive\" : true,\n"
            + "            \"boost\" : 1.0,\n"
            + "            \"_name\" : \"myTestField\"\n"
            + "          }\n"
            + "        }\n"
            + "      }\n"
            + "    ],\n"
            + "    \"adjust_pure_negative\" : true,\n"
            + "    \"minimum_should_match\" : \"1\",\n"
            + "    \"boost\" : 1.0\n"
            + "  }\n"
            + "}";

    Assert.assertEquals(result.toString(), expected);
  }

  @Test
  public void testWildcardQueryBuilderFromCriterionWhenStartsWith() {
    final Criterion singleValueCriterion =
        buildCriterion("myTestField", Condition.START_WITH, "value1");

    QueryBuilder result =
        ESUtils.getQueryBuilderFromCriterion(
            singleValueCriterion,
            false,
            new HashMap<>(),
            mock(OperationContext.class),
            mock(QueryFilterRewriteChain.class));

    String expected =
        "{\n"
            + "  \"bool\" : {\n"
            + "    \"should\" : [\n"
            + "      {\n"
            + "        \"wildcard\" : {\n"
            + "          \"myTestField.keyword\" : {\n"
            + "            \"wildcard\" : \"value1*\",\n"
            + "            \"case_insensitive\" : true,\n"
            + "            \"boost\" : 1.0,\n"
            + "            \"_name\" : \"myTestField\"\n"
            + "          }\n"
            + "        }\n"
            + "      }\n"
            + "    ],\n"
            + "    \"adjust_pure_negative\" : true,\n"
            + "    \"minimum_should_match\" : \"1\",\n"
            + "    \"boost\" : 1.0\n"
            + "  }\n"
            + "}";

    Assert.assertEquals(result.toString(), expected);

    final Criterion multiValueCriterion =
        buildCriterion("myTestField", Condition.START_WITH, "value1", "value2");

    result =
        ESUtils.getQueryBuilderFromCriterion(
            multiValueCriterion,
            false,
            new HashMap<>(),
            mock(OperationContext.class),
            mock(QueryFilterRewriteChain.class));

    expected =
        "{\n"
            + "  \"bool\" : {\n"
            + "    \"should\" : [\n"
            + "      {\n"
            + "        \"wildcard\" : {\n"
            + "          \"myTestField.keyword\" : {\n"
            + "            \"wildcard\" : \"value1*\",\n"
            + "            \"case_insensitive\" : true,\n"
            + "            \"boost\" : 1.0,\n"
            + "            \"_name\" : \"myTestField\"\n"
            + "          }\n"
            + "        }\n"
            + "      },\n"
            + "      {\n"
            + "        \"wildcard\" : {\n"
            + "          \"myTestField.keyword\" : {\n"
            + "            \"wildcard\" : \"value2*\",\n"
            + "            \"case_insensitive\" : true,\n"
            + "            \"boost\" : 1.0,\n"
            + "            \"_name\" : \"myTestField\"\n"
            + "          }\n"
            + "        }\n"
            + "      }\n"
            + "    ],\n"
            + "    \"adjust_pure_negative\" : true,\n"
            + "    \"minimum_should_match\" : \"1\",\n"
            + "    \"boost\" : 1.0\n"
            + "  }\n"
            + "}";

    Assert.assertEquals(result.toString(), expected);
  }

  @Test
  public void testWildcardQueryBuilderFromCriterionWhenEndsWith() {
    final Criterion singleValueCriterion =
        buildCriterion("myTestField", Condition.END_WITH, "value1");

    QueryBuilder result =
        ESUtils.getQueryBuilderFromCriterion(
            singleValueCriterion,
            false,
            new HashMap<>(),
            mock(OperationContext.class),
            mock(QueryFilterRewriteChain.class));

    String expected =
        "{\n"
            + "  \"bool\" : {\n"
            + "    \"should\" : [\n"
            + "      {\n"
            + "        \"wildcard\" : {\n"
            + "          \"myTestField.keyword\" : {\n"
            + "            \"wildcard\" : \"*value1\",\n"
            + "            \"case_insensitive\" : true,\n"
            + "            \"boost\" : 1.0,\n"
            + "            \"_name\" : \"myTestField\"\n"
            + "          }\n"
            + "        }\n"
            + "      }\n"
            + "    ],\n"
            + "    \"adjust_pure_negative\" : true,\n"
            + "    \"minimum_should_match\" : \"1\",\n"
            + "    \"boost\" : 1.0\n"
            + "  }\n"
            + "}";
    Assert.assertEquals(result.toString(), expected);

    final Criterion multiValueCriterion =
        buildCriterion("myTestField", Condition.END_WITH, "value1", "value2");

    result =
        ESUtils.getQueryBuilderFromCriterion(
            multiValueCriterion,
            false,
            new HashMap<>(),
            mock(OperationContext.class),
            mock(QueryFilterRewriteChain.class));

    expected =
        "{\n"
            + "  \"bool\" : {\n"
            + "    \"should\" : [\n"
            + "      {\n"
            + "        \"wildcard\" : {\n"
            + "          \"myTestField.keyword\" : {\n"
            + "            \"wildcard\" : \"*value1\",\n"
            + "            \"case_insensitive\" : true,\n"
            + "            \"boost\" : 1.0,\n"
            + "            \"_name\" : \"myTestField\"\n"
            + "          }\n"
            + "        }\n"
            + "      },\n"
            + "      {\n"
            + "        \"wildcard\" : {\n"
            + "          \"myTestField.keyword\" : {\n"
            + "            \"wildcard\" : \"*value2\",\n"
            + "            \"case_insensitive\" : true,\n"
            + "            \"boost\" : 1.0,\n"
            + "            \"_name\" : \"myTestField\"\n"
            + "          }\n"
            + "        }\n"
            + "      }\n"
            + "    ],\n"
            + "    \"adjust_pure_negative\" : true,\n"
            + "    \"minimum_should_match\" : \"1\",\n"
            + "    \"boost\" : 1.0\n"
            + "  }\n"
            + "}";

    Assert.assertEquals(result.toString(), expected);
  }

  @Test
  public void testGetQueryBuilderFromCriterionExists() {
    final Criterion singleValueCriterion = buildExistsCriterion("myTestField");

    QueryBuilder result =
        ESUtils.getQueryBuilderFromCriterion(
            singleValueCriterion,
            false,
            new HashMap<>(),
            mock(OperationContext.class),
            QueryFilterRewriteChain.EMPTY);
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
    final Criterion timeseriesField = buildExistsCriterion("myTestField");

    result =
        ESUtils.getQueryBuilderFromCriterion(
            timeseriesField,
            true,
            new HashMap<>(),
            mock(OperationContext.class),
            QueryFilterRewriteChain.EMPTY);
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
    final Criterion singleValueCriterion = buildIsNullCriterion("myTestField");

    QueryBuilder result =
        ESUtils.getQueryBuilderFromCriterion(
            singleValueCriterion,
            false,
            new HashMap<>(),
            mock(OperationContext.class),
            QueryFilterRewriteChain.EMPTY);
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
    final Criterion timeseriesField = buildIsNullCriterion("myTestField");

    result =
        ESUtils.getQueryBuilderFromCriterion(
            timeseriesField,
            true,
            new HashMap<>(),
            mock(OperationContext.class),
            QueryFilterRewriteChain.EMPTY);
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
        buildCriterion(FIELD_TO_EXPAND, Condition.EQUAL, "value1");

    // Ensure that the query is expanded!
    QueryBuilder result =
        ESUtils.getQueryBuilderFromCriterion(
            singleValueCriterion,
            false,
            new HashMap<>(),
            mock(OperationContext.class),
            QueryFilterRewriteChain.EMPTY);
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
            + "    \"minimum_should_match\" : \"1\",\n"
            + "    \"boost\" : 1.0\n"
            + "  }\n"
            + "}";
    Assert.assertEquals(result.toString(), expected);

    final Criterion timeseriesField =
        buildCriterion(FIELD_TO_EXPAND, Condition.EQUAL, "value1", "value2");

    // Ensure that the query is expanded without keyword.
    result =
        ESUtils.getQueryBuilderFromCriterion(
            timeseriesField,
            true,
            new HashMap<>(),
            mock(OperationContext.class),
            QueryFilterRewriteChain.EMPTY);
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
            + "    \"minimum_should_match\" : \"1\",\n"
            + "    \"boost\" : 1.0\n"
            + "  }\n"
            + "}";
    Assert.assertEquals(result.toString(), expected);

    final Criterion originCriterion = buildCriterion("origin", Condition.EQUAL, "PROD");

    // Ensure that the query is expanded!
    QueryBuilder originExpanded =
        ESUtils.getQueryBuilderFromCriterion(
            originCriterion,
            false,
            new HashMap<>(),
            mock(OperationContext.class),
            QueryFilterRewriteChain.EMPTY);
    String originExpected =
        "{\n"
            + "  \"bool\" : {\n"
            + "    \"should\" : [\n"
            + "      {\n"
            + "        \"terms\" : {\n"
            + "          \"origin.keyword\" : [\n"
            + "            \"PROD\"\n"
            + "          ],\n"
            + "          \"boost\" : 1.0,\n"
            + "          \"_name\" : \"origin\"\n"
            + "        }\n"
            + "      },\n"
            + "      {\n"
            + "        \"terms\" : {\n"
            + "          \"env.keyword\" : [\n"
            + "            \"PROD\"\n"
            + "          ],\n"
            + "          \"boost\" : 1.0,\n"
            + "          \"_name\" : \"env\"\n"
            + "        }\n"
            + "      }\n"
            + "    ],\n"
            + "    \"adjust_pure_negative\" : true,\n"
            + "    \"minimum_should_match\" : \"1\",\n"
            + "    \"boost\" : 1.0\n"
            + "  }\n"
            + "}";
    Assert.assertEquals(originExpanded.toString(), originExpected);

    final Criterion envCriterion = buildCriterion("env", Condition.EQUAL, "PROD");

    // Ensure that the query is expanded!
    QueryBuilder envExpanded =
        ESUtils.getQueryBuilderFromCriterion(
            envCriterion,
            false,
            new HashMap<>(),
            mock(OperationContext.class),
            QueryFilterRewriteChain.EMPTY);
    String envExpected =
        "{\n"
            + "  \"bool\" : {\n"
            + "    \"should\" : [\n"
            + "      {\n"
            + "        \"terms\" : {\n"
            + "          \"env.keyword\" : [\n"
            + "            \"PROD\"\n"
            + "          ],\n"
            + "          \"boost\" : 1.0,\n"
            + "          \"_name\" : \"env\"\n"
            + "        }\n"
            + "      },\n"
            + "      {\n"
            + "        \"terms\" : {\n"
            + "          \"origin.keyword\" : [\n"
            + "            \"PROD\"\n"
            + "          ],\n"
            + "          \"boost\" : 1.0,\n"
            + "          \"_name\" : \"origin\"\n"
            + "        }\n"
            + "      }\n"
            + "    ],\n"
            + "    \"adjust_pure_negative\" : true,\n"
            + "    \"minimum_should_match\" : \"1\",\n"
            + "    \"boost\" : 1.0\n"
            + "  }\n"
            + "}";
    Assert.assertEquals(envExpanded.toString(), envExpected);
  }

  @Test
  public void testGetQueryBuilderFromStructPropEqualsValue() {

    final Criterion singleValueCriterion =
        buildCriterion("structuredProperties.ab.fgh.ten", Condition.EQUAL, "value1");

    OperationContext opContext = mock(OperationContext.class);
    when(opContext.getAspectRetriever()).thenReturn(aspectRetriever);
    QueryBuilder result =
        ESUtils.getQueryBuilderFromCriterion(
            singleValueCriterion, false, new HashMap<>(), opContext, QueryFilterRewriteChain.EMPTY);
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
  public void testGetQueryBuilderFromNamespacedStructPropEqualsValue() {

    final Criterion singleValueCriterion =
        buildCriterion(
            "structuredProperties.under.scores.and.dots_make_a_mess", Condition.EQUAL, "value1");

    OperationContext opContext = mock(OperationContext.class);
    when(opContext.getAspectRetriever()).thenReturn(aspectRetriever);
    QueryBuilder result =
        ESUtils.getQueryBuilderFromCriterion(
            singleValueCriterion, false, new HashMap<>(), opContext, QueryFilterRewriteChain.EMPTY);
    String expected =
        "{\n"
            + "  \"terms\" : {\n"
            + "    \"structuredProperties.under_scores_and_dots_make_a_mess.keyword\" : [\n"
            + "      \"value1\"\n"
            + "    ],\n"
            + "    \"boost\" : 1.0,\n"
            + "    \"_name\" : \"structuredProperties.under.scores.and.dots_make_a_mess\"\n"
            + "  }\n"
            + "}";
    Assert.assertEquals(result.toString(), expected);
  }

  @Test
  public void testGetQueryBuilderFromStructPropEqualsValueV1() {

    final Criterion singleValueCriterion =
        buildCriterion("structuredProperties.ab.fgh.ten", Condition.EQUAL, "value1");

    OperationContext opContextV1 = mock(OperationContext.class);
    when(opContextV1.getAspectRetriever()).thenReturn(aspectRetrieverV1);
    QueryBuilder result =
        ESUtils.getQueryBuilderFromCriterion(
            singleValueCriterion,
            false,
            new HashMap<>(),
            opContextV1,
            QueryFilterRewriteChain.EMPTY);
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
  public void testGetQueryBuilderFromNamespacedStructPropEqualsValueV1() {

    final Criterion singleValueCriterion =
        buildCriterion(
            "structuredProperties.under.scores.and.dots_make_a_mess", Condition.EQUAL, "value1");

    OperationContext opContextV1 = mock(OperationContext.class);
    when(opContextV1.getAspectRetriever()).thenReturn(aspectRetrieverV1);
    QueryBuilder result =
        ESUtils.getQueryBuilderFromCriterion(
            singleValueCriterion,
            false,
            new HashMap<>(),
            opContextV1,
            QueryFilterRewriteChain.EMPTY);
    String expected =
        "{\n"
            + "  \"terms\" : {\n"
            + "    \"structuredProperties._versioned.under_scores_and_dots_make_a_mess.00000000000001.string.keyword\" : [\n"
            + "      \"value1\"\n"
            + "    ],\n"
            + "    \"boost\" : 1.0,\n"
            + "    \"_name\" : \"structuredProperties.under.scores.and.dots_make_a_mess\"\n"
            + "  }\n"
            + "}";
    Assert.assertEquals(result.toString(), expected);
  }

  @Test
  public void testGetQueryBuilderFromDatesWithDots() {

    final Criterion singleValueCriterion =
        buildCriterion(
            "structuredProperties.date_here.with_dot", Condition.GREATER_THAN, "1731974400000");

    OperationContext opContext = mock(OperationContext.class);
    when(opContext.getAspectRetriever()).thenReturn(aspectRetriever);
    QueryBuilder result =
        ESUtils.getQueryBuilderFromCriterion(
            singleValueCriterion, false, new HashMap<>(), opContext, QueryFilterRewriteChain.EMPTY);
    // structuredProperties.date_here_with_dot should not have .keyword at the end since this field
    // type is type long for dates
    String expected =
        "{\n"
            + "  \"range\" : {\n"
            + "    \"structuredProperties.date_here_with_dot\" : {\n"
            + "      \"from\" : 1731974400000,\n"
            + "      \"to\" : null,\n"
            + "      \"include_lower\" : false,\n"
            + "      \"include_upper\" : true,\n"
            + "      \"boost\" : 1.0,\n"
            + "      \"_name\" : \"structuredProperties.date_here.with_dot\"\n"
            + "    }\n"
            + "  }\n"
            + "}";
    Assert.assertEquals(result.toString(), expected);
  }

  @Test
  public void testGetQueryBuilderFromStructPropExists() {
    final Criterion singleValueCriterion = buildExistsCriterion("structuredProperties.ab.fgh.ten");

    OperationContext opContext = mock(OperationContext.class);
    when(opContext.getAspectRetriever()).thenReturn(aspectRetriever);
    QueryBuilder result =
        ESUtils.getQueryBuilderFromCriterion(
            singleValueCriterion, false, new HashMap<>(), opContext, QueryFilterRewriteChain.EMPTY);
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
    final Criterion timeseriesField = buildExistsCriterion("myTestField");

    result =
        ESUtils.getQueryBuilderFromCriterion(
            timeseriesField, true, new HashMap<>(), opContext, QueryFilterRewriteChain.EMPTY);
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
    final Criterion singleValueCriterion = buildExistsCriterion("structuredProperties.ab.fgh.ten");

    OperationContext opContextV1 = mock(OperationContext.class);
    when(opContextV1.getAspectRetriever()).thenReturn(aspectRetrieverV1);
    QueryBuilder result =
        ESUtils.getQueryBuilderFromCriterion(
            singleValueCriterion,
            false,
            new HashMap<>(),
            opContextV1,
            QueryFilterRewriteChain.EMPTY);
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
    final Criterion timeseriesField = buildCriterion("myTestField", Condition.EXISTS);

    result =
        ESUtils.getQueryBuilderFromCriterion(
            timeseriesField, true, new HashMap<>(), opContextV1, QueryFilterRewriteChain.EMPTY);
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
  public void testGetQueryBuilderForObjectFields() {
    final Criterion singleValueCriterion =
        new Criterion()
            .setField("testObjectField.numericField")
            .setCondition(Condition.EQUAL)
            .setValues(new StringArray(ImmutableList.of("10")));

    Map<String, Set<SearchableAnnotation.FieldType>> searchableFieldTypes = new HashMap<>();
    searchableFieldTypes.put("testObjectField", Set.of(SearchableAnnotation.FieldType.DOUBLE));

    QueryBuilder result =
        ESUtils.getQueryBuilderFromCriterion(
            singleValueCriterion,
            false,
            searchableFieldTypes,
            mock(OperationContext.class),
            QueryFilterRewriteChain.EMPTY);
    String expected =
        "{\n"
            + "  \"terms\" : {\n"
            + "    \"testObjectField.numericField\" : [\n"
            + "      10.0\n"
            + "    ],\n"
            + "    \"boost\" : 1.0,\n"
            + "    \"_name\" : \"testObjectField.numericField\"\n"
            + "  }\n"
            + "}";
    Assert.assertEquals(result.toString(), expected);

    final Criterion multiValueCriterion =
        new Criterion()
            .setField("testObjectField.numericField")
            .setCondition(Condition.EQUAL)
            .setValues(new StringArray(ImmutableList.of("10", "20")));

    result =
        ESUtils.getQueryBuilderFromCriterion(
            multiValueCriterion,
            false,
            searchableFieldTypes,
            mock(OperationContext.class),
            QueryFilterRewriteChain.EMPTY);
    expected =
        "{\n"
            + "  \"terms\" : {\n"
            + "    \"testObjectField.numericField\" : [\n"
            + "      10.0,\n"
            + "      20.0\n"
            + "    ],\n"
            + "    \"boost\" : 1.0,\n"
            + "    \"_name\" : \"testObjectField.numericField\"\n"
            + "  }\n"
            + "}";
    Assert.assertEquals(result.toString(), expected);
  }
}
