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
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.index.query.TermsQueryBuilder;
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

    final Criterion businessAttributeRefNestedFieldCriterion =
        buildCriterion("businessAttribute", Condition.EQUAL, "urn:li:businessAttribute:value");

    QueryBuilder businessAttributeExpanded =
        ESUtils.getQueryBuilderFromCriterion(
            businessAttributeRefNestedFieldCriterion,
            false,
            new HashMap<>(),
            mock(OperationContext.class),
            QueryFilterRewriteChain.EMPTY);
    String businessAttributeExpected =
        "{\n"
            + "  \"bool\" : {\n"
            + "    \"should\" : [\n"
            + "      {\n"
            + "        \"terms\" : {\n"
            + "          \"businessAttributeRef.keyword\" : [\n"
            + "            \"urn:li:businessAttribute:value\"\n"
            + "          ],\n"
            + "          \"boost\" : 1.0,\n"
            + "          \"_name\" : \"businessAttributeRef\"\n"
            + "        }\n"
            + "      },\n"
            + "      {\n"
            + "        \"terms\" : {\n"
            + "          \"businessAttributeRef.urn\" : [\n"
            + "            \"urn:li:businessAttribute:value\"\n"
            + "          ],\n"
            + "          \"boost\" : 1.0,\n"
            + "          \"_name\" : \"businessAttributeRef.urn\"\n"
            + "        }\n"
            + "      }\n"
            + "    ],\n"
            + "    \"adjust_pure_negative\" : true,\n"
            + "    \"minimum_should_match\" : \"1\",\n"
            + "    \"boost\" : 1.0\n"
            + "  }\n"
            + "}";
    Assert.assertEquals(businessAttributeExpanded.toString(), businessAttributeExpected);
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

  @Test
  public void testOptimizePassWithSingleShouldClause() {
    // Test the basic should clause optimization
    BoolQueryBuilder boolQuery =
        QueryBuilders.boolQuery()
            .should(QueryBuilders.termQuery("field1", "value1"))
            .minimumShouldMatch("1");

    boolean changed = ESUtils.optimizePass(boolQuery, true);
    Assert.assertTrue(changed);
    Assert.assertEquals(0, boolQuery.should().size());
    Assert.assertEquals(1, boolQuery.must().size());
    Assert.assertNull(boolQuery.minimumShouldMatch());
  }

  @Test
  public void testOptimizePassWithSingleShouldClauseToFilter() {
    // Test should clause optimization when considerScore is false
    BoolQueryBuilder boolQuery =
        QueryBuilders.boolQuery()
            .should(QueryBuilders.termQuery("field1", "value1"))
            .minimumShouldMatch("1");

    boolean changed = ESUtils.optimizePass(boolQuery, false);
    Assert.assertTrue(changed);
    Assert.assertEquals(0, boolQuery.should().size());
    Assert.assertEquals(1, boolQuery.filter().size());
    Assert.assertNull(boolQuery.minimumShouldMatch());
  }

  @Test
  public void testOptimizePassWithPercentageMinimumShouldMatch() {
    // Test with 100% minimum should match
    BoolQueryBuilder boolQuery =
        QueryBuilders.boolQuery()
            .should(QueryBuilders.termQuery("field1", "value1"))
            .minimumShouldMatch("100%");

    boolean changed = ESUtils.optimizePass(boolQuery, true);
    Assert.assertTrue(changed);
    Assert.assertEquals(0, boolQuery.should().size());
    Assert.assertEquals(1, boolQuery.must().size());
  }

  @Test
  public void testOptimizePassNoOptimizationMultipleShouldClauses() {
    // Test that optimization doesn't happen with multiple should clauses
    BoolQueryBuilder boolQuery =
        QueryBuilders.boolQuery()
            .should(QueryBuilders.termQuery("field1", "value1"))
            .should(QueryBuilders.termQuery("field2", "value2"))
            .minimumShouldMatch("1");

    boolean changed = ESUtils.optimizePass(boolQuery, true);
    Assert.assertFalse(changed);
    Assert.assertEquals(2, boolQuery.should().size());
  }

  @Test
  public void testOptimizePassNoOptimizationNoMinimumShouldMatch() {
    // Test that optimization doesn't happen without minimumShouldMatch
    BoolQueryBuilder boolQuery =
        QueryBuilders.boolQuery().should(QueryBuilders.termQuery("field1", "value1"));

    boolean changed = ESUtils.optimizePass(boolQuery, true);
    Assert.assertFalse(changed);
    Assert.assertEquals(1, boolQuery.should().size());
  }

  @Test
  public void testOptimizePassFlattenNestedFilters() {
    // Test flattening of nested filter clauses
    BoolQueryBuilder innerBool =
        QueryBuilders.boolQuery()
            .filter(QueryBuilders.termQuery("field1", "value1"))
            .filter(QueryBuilders.termQuery("field2", "value2"));

    BoolQueryBuilder outerBool = QueryBuilders.boolQuery().filter(innerBool);

    boolean changed = ESUtils.optimizePass(outerBool, false);
    Assert.assertTrue(changed);
    Assert.assertEquals(2, outerBool.filter().size());
    // Check that the filters are now at the top level
    Assert.assertTrue(outerBool.filter().stream().allMatch(q -> q instanceof TermQueryBuilder));
  }

  @Test
  public void testOptimizePassDeeplyNestedFilters() {
    // Test flattening of deeply nested filter clauses
    BoolQueryBuilder level3 =
        QueryBuilders.boolQuery()
            .filter(QueryBuilders.termsQuery("source.urn", "urn:li:dataset:test-dataset-1"))
            .filter(QueryBuilders.termQuery("relationshipType", "DownstreamOf"))
            .filter(QueryBuilders.termsQuery("destination.entityType", "dataset"));

    BoolQueryBuilder level2 = QueryBuilders.boolQuery().filter(level3);

    BoolQueryBuilder level1 = QueryBuilders.boolQuery().filter(level2);

    BoolQueryBuilder rootQuery = QueryBuilders.boolQuery().filter(level1);

    // Run multiple optimization passes
    boolean changed = false;
    do {
      changed = ESUtils.optimizePass(rootQuery, false);
    } while (changed);

    // All filters should now be at the root level
    Assert.assertEquals(3, rootQuery.filter().size());
    Assert.assertEquals(0, rootQuery.must().size());
    Assert.assertEquals(0, rootQuery.should().size());
    Assert.assertEquals(0, rootQuery.mustNot().size());
  }

  @Test
  public void testOptimizePassMixedBoolQuery() {
    // Test that filters with must/should/mustNot clauses are not flattened
    BoolQueryBuilder innerBool =
        QueryBuilders.boolQuery()
            .must(QueryBuilders.termQuery("field1", "value1"))
            .filter(QueryBuilders.termQuery("field2", "value2"))
            .should(QueryBuilders.termQuery("field3", "value3"));

    BoolQueryBuilder outerBool = QueryBuilders.boolQuery().filter(innerBool);

    boolean changed = ESUtils.optimizePass(outerBool, false);
    Assert.assertFalse(changed);
    Assert.assertEquals(1, outerBool.filter().size());
    Assert.assertEquals(innerBool, outerBool.filter().get(0));
  }

  @Test
  public void testOptimizePassRecursiveOptimization() {
    // Test that nested queries are optimized recursively
    BoolQueryBuilder innerBool =
        QueryBuilders.boolQuery()
            .should(QueryBuilders.termQuery("field1", "value1"))
            .minimumShouldMatch("1");

    BoolQueryBuilder outerBool = QueryBuilders.boolQuery().must(innerBool);

    boolean changed = ESUtils.optimizePass(outerBool, true);
    Assert.assertTrue(changed);

    // The inner bool query should have been optimized
    BoolQueryBuilder optimizedInner = (BoolQueryBuilder) outerBool.must().get(0);
    Assert.assertEquals(0, optimizedInner.should().size());
    Assert.assertEquals(1, optimizedInner.must().size());
  }

  @Test
  public void testOptimizeFullQuery() {
    // Test the full optimize method
    BoolQueryBuilder boolQuery =
        QueryBuilders.boolQuery()
            .should(QueryBuilders.termQuery("field1", "value1"))
            .minimumShouldMatch("1");

    QueryBuilder result = ESUtils.queryOptimize(boolQuery, true);

    // Should be unwrapped to just the term query
    Assert.assertTrue(result instanceof TermQueryBuilder);
    TermQueryBuilder termQuery = (TermQueryBuilder) result;
    Assert.assertEquals("field1", termQuery.fieldName());
    Assert.assertEquals("value1", termQuery.value());
  }

  @Test
  public void testOptimizeWithSingleFilterUnwrap() {
    // Test unwrapping of bool query with single filter
    BoolQueryBuilder boolQuery =
        QueryBuilders.boolQuery().filter(QueryBuilders.termQuery("field1", "value1"));

    QueryBuilder result = ESUtils.queryOptimize(boolQuery, false);

    // Should be unwrapped to just the term query
    Assert.assertTrue(result instanceof TermQueryBuilder);
    TermQueryBuilder termQuery = (TermQueryBuilder) result;
    Assert.assertEquals("field1", termQuery.fieldName());
  }

  @Test
  public void testOptimizeWithSingleMustNotNoUnwrap() {
    // Test that single mustNot clause is not unwrapped (needs bool wrapper)
    BoolQueryBuilder boolQuery =
        QueryBuilders.boolQuery().mustNot(QueryBuilders.termQuery("field1", "value1"));

    QueryBuilder result = ESUtils.queryOptimize(boolQuery, false);

    // Should remain a bool query
    Assert.assertTrue(result instanceof BoolQueryBuilder);
    BoolQueryBuilder resultBool = (BoolQueryBuilder) result;
    Assert.assertEquals(1, resultBool.mustNot().size());
  }

  @Test
  public void testOptimizeNullQuery() {
    // Test null handling
    QueryBuilder result = ESUtils.queryOptimize(null, false);
    Assert.assertNull(result);
  }

  @Test
  public void testOptimizeNonBoolQuery() {
    // Test that non-bool queries are returned unchanged
    TermQueryBuilder termQuery = QueryBuilders.termQuery("field1", "value1");
    QueryBuilder result = ESUtils.queryOptimize(termQuery, false);
    Assert.assertEquals(termQuery, result);
  }

  @Test
  public void testOptimizeComplexNestedStructure() {
    // Test a complex nested structure with multiple optimizations
    BoolQueryBuilder innerFilter =
        QueryBuilders.boolQuery().filter(QueryBuilders.termQuery("field1", "value1"));

    BoolQueryBuilder innerShould =
        QueryBuilders.boolQuery()
            .should(QueryBuilders.termQuery("field2", "value2"))
            .minimumShouldMatch("1");

    BoolQueryBuilder middleQuery = QueryBuilders.boolQuery().filter(innerFilter).must(innerShould);

    BoolQueryBuilder outerQuery = QueryBuilders.boolQuery().filter(middleQuery);

    // Use the public queryOptimize method instead of manually running passes
    QueryBuilder result = ESUtils.queryOptimize(outerQuery, false);

    // The result should be optimized
    Assert.assertNotNull(result);

    // After full optimization, we expect:
    // 1. innerShould's should clause converted to must (then to filter since considerScore=false)
    // 2. innerFilter's filter flattened into middleQuery
    // 3. middleQuery's filters flattened into outerQuery
    // 4. Possible unwrapping if only one clause remains

    // The exact structure depends on the optimization order, but we should not have
    // deeply nested bool queries with only filters
    if (result instanceof BoolQueryBuilder) {
      BoolQueryBuilder optimized = (BoolQueryBuilder) result;

      // Check that we don't have nested bool queries with only filters
      for (QueryBuilder clause : optimized.filter()) {
        if (clause instanceof BoolQueryBuilder) {
          BoolQueryBuilder nestedBool = (BoolQueryBuilder) clause;
          // If there's a nested bool, it should have more than just filters
          boolean hasNonFilterClauses =
              !nestedBool.must().isEmpty()
                  || !nestedBool.should().isEmpty()
                  || !nestedBool.mustNot().isEmpty();
          Assert.assertTrue(
              hasNonFilterClauses || nestedBool.filter().isEmpty(),
              "Nested bool query should not have only filter clauses");
        }
      }
    }
  }

  @Test
  public void testIsOptimizableWithVariousMinimumShouldMatch() {
    // Test various minimumShouldMatch formats
    BoolQueryBuilder query1 =
        QueryBuilders.boolQuery()
            .should(QueryBuilders.termQuery("field", "value"))
            .minimumShouldMatch("1");
    Assert.assertTrue(ESUtils.isOptimizableShould(query1));

    BoolQueryBuilder query2 =
        QueryBuilders.boolQuery()
            .should(QueryBuilders.termQuery("field", "value"))
            .minimumShouldMatch("100%");
    Assert.assertTrue(ESUtils.isOptimizableShould(query2));

    BoolQueryBuilder query3 =
        QueryBuilders.boolQuery()
            .should(QueryBuilders.termQuery("field", "value"))
            .minimumShouldMatch("50%");
    Assert.assertFalse(ESUtils.isOptimizableShould(query3));

    BoolQueryBuilder query4 =
        QueryBuilders.boolQuery()
            .should(QueryBuilders.termQuery("field", "value"))
            .minimumShouldMatch("2");
    Assert.assertFalse(ESUtils.isOptimizableShould(query4));
  }

  @Test
  public void testCanFlattenFilters() {
    // Test the canFlattenFilters method
    BoolQueryBuilder flattenable =
        QueryBuilders.boolQuery()
            .filter(QueryBuilders.boolQuery().filter(QueryBuilders.termQuery("field1", "value1")));
    Assert.assertTrue(ESUtils.canFlattenFilters(flattenable));

    BoolQueryBuilder notFlattenable =
        QueryBuilders.boolQuery()
            .filter(QueryBuilders.boolQuery().must(QueryBuilders.termQuery("field1", "value1")));
    Assert.assertFalse(ESUtils.canFlattenFilters(notFlattenable));

    BoolQueryBuilder noNestedBool =
        QueryBuilders.boolQuery().filter(QueryBuilders.termQuery("field1", "value1"));
    Assert.assertFalse(ESUtils.canFlattenFilters(noNestedBool));
  }

  @Test
  public void testCanUnwrap() {
    // Test the canUnwrap method
    BoolQueryBuilder singleMust =
        QueryBuilders.boolQuery().must(QueryBuilders.termQuery("field", "value"));
    Assert.assertTrue(ESUtils.canUnwrap(singleMust));

    BoolQueryBuilder singleFilter =
        QueryBuilders.boolQuery().filter(QueryBuilders.termQuery("field", "value"));
    Assert.assertTrue(ESUtils.canUnwrap(singleFilter));

    BoolQueryBuilder multipleClauses =
        QueryBuilders.boolQuery()
            .must(QueryBuilders.termQuery("field1", "value1"))
            .filter(QueryBuilders.termQuery("field2", "value2"));
    Assert.assertFalse(ESUtils.canUnwrap(multipleClauses));

    BoolQueryBuilder withMinShouldMatch =
        QueryBuilders.boolQuery()
            .should(QueryBuilders.termQuery("field", "value"))
            .minimumShouldMatch("1");
    Assert.assertFalse(ESUtils.canUnwrap(withMinShouldMatch));
  }

  @Test
  public void testOptimizeComplexNestedShouldWithFilters() {
    // Build the complex nested structure programmatically

    // First branch: DownstreamOf relationship
    BoolQueryBuilder downstreamQuery =
        QueryBuilders.boolQuery()
            .filter(
                QueryBuilders.termsQuery(
                    "source.urn", "urn:li:dataset:(urn:li:dataPlatform:hive,logging_events,PROD)"))
            .filter(QueryBuilders.termQuery("relationshipType", "DownstreamOf"))
            .filter(QueryBuilders.termsQuery("destination.entityType", "dataset"));

    // Second branch: DataProcessInstanceProduces relationship
    BoolQueryBuilder dataProcessQuery =
        QueryBuilders.boolQuery()
            .filter(
                QueryBuilders.termsQuery(
                    "destination.urn",
                    "urn:li:dataset:(urn:li:dataPlatform:hive,logging_events,PROD)"))
            .filter(QueryBuilders.termQuery("relationshipType", "DataProcessInstanceProduces"))
            .filter(QueryBuilders.termsQuery("source.entityType", "dataProcessInstance"));

    // Third branch: Produces relationship
    BoolQueryBuilder producesQuery =
        QueryBuilders.boolQuery()
            .filter(
                QueryBuilders.termsQuery(
                    "destination.urn",
                    "urn:li:dataset:(urn:li:dataPlatform:hive,logging_events,PROD)"))
            .filter(QueryBuilders.termQuery("relationshipType", "Produces"))
            .filter(QueryBuilders.termsQuery("source.entityType", "dataJob"));

    // Inner should with three branches
    BoolQueryBuilder innerShould =
        QueryBuilders.boolQuery()
            .should(downstreamQuery)
            .should(dataProcessQuery)
            .should(producesQuery)
            .minimumShouldMatch("1");

    // Outer should with single clause
    BoolQueryBuilder outerShould =
        QueryBuilders.boolQuery().should(innerShould).minimumShouldMatch("1");

    // Root query with filter
    BoolQueryBuilder rootQuery = QueryBuilders.boolQuery().filter(outerShould);

    // Optimize the query
    QueryBuilder optimized = ESUtils.queryOptimize(rootQuery, false);

    // Verify the optimization results
    Assert.assertTrue(optimized instanceof BoolQueryBuilder);
    BoolQueryBuilder optimizedBool = (BoolQueryBuilder) optimized;

    // After optimization:
    // 1. Outer should with 1 clause -> converted to filter
    // 2. Inner should with 1 clause -> converted to filter
    // 3. Multiple unwrapping steps bring the innermost bool to the root
    // Result: The inner bool query with 3 should clauses is now at the root

    Assert.assertEquals(0, optimizedBool.filter().size(), "Should have no filter clauses");
    Assert.assertEquals(0, optimizedBool.must().size(), "Should have no must clauses");
    Assert.assertEquals(3, optimizedBool.should().size(), "Should have 3 should clauses at root");
    Assert.assertEquals(0, optimizedBool.mustNot().size(), "Should have no mustNot clauses");
    Assert.assertEquals(
        "1", optimizedBool.minimumShouldMatch(), "Should have minimumShouldMatch=1");

    // Each of the 3 should clauses should be a bool query with 3 filters
    for (QueryBuilder shouldClause : optimizedBool.should()) {
      Assert.assertTrue(shouldClause instanceof BoolQueryBuilder);
      BoolQueryBuilder shouldBool = (BoolQueryBuilder) shouldClause;
      Assert.assertEquals(
          3, shouldBool.filter().size(), "Each branch should have 3 filter clauses");
      Assert.assertEquals(0, shouldBool.must().size());
      Assert.assertEquals(0, shouldBool.should().size());
      Assert.assertEquals(0, shouldBool.mustNot().size());
    }

    // Verify the actual filters in each branch
    BoolQueryBuilder branch1 = (BoolQueryBuilder) optimizedBool.should().get(0);
    Assert.assertTrue(
        branch1.filter().stream()
            .anyMatch(
                f ->
                    f instanceof TermsQueryBuilder
                        && ((TermsQueryBuilder) f).fieldName().equals("source.urn")));

    BoolQueryBuilder branch2 = (BoolQueryBuilder) optimizedBool.should().get(1);
    Assert.assertTrue(
        branch2.filter().stream()
            .anyMatch(
                f ->
                    f instanceof TermQueryBuilder
                        && ((TermQueryBuilder) f).value().equals("DataProcessInstanceProduces")));

    BoolQueryBuilder branch3 = (BoolQueryBuilder) optimizedBool.should().get(2);
    Assert.assertTrue(
        branch3.filter().stream()
            .anyMatch(
                f ->
                    f instanceof TermQueryBuilder
                        && ((TermQueryBuilder) f).value().equals("Produces")));
  }

  @Test
  public void testOptimizeComplexQueryFullyFlattened() {
    // Test a variant where all optimizations can be applied

    // Create a deeply nested structure with single should clauses that can be optimized
    BoolQueryBuilder leaf1 =
        QueryBuilders.boolQuery().filter(QueryBuilders.termQuery("field1", "value1"));

    BoolQueryBuilder leaf2 =
        QueryBuilders.boolQuery().filter(QueryBuilders.termQuery("field2", "value2"));

    // Single should that can be optimized
    BoolQueryBuilder middle1 = QueryBuilders.boolQuery().should(leaf1).minimumShouldMatch("1");

    // Single should that can be optimized
    BoolQueryBuilder middle2 = QueryBuilders.boolQuery().should(leaf2).minimumShouldMatch("1");

    // Bool with only filters that can be flattened
    BoolQueryBuilder filterOnly = QueryBuilders.boolQuery().filter(middle1).filter(middle2);

    // Root query
    BoolQueryBuilder rootQuery = QueryBuilders.boolQuery().filter(filterOnly);

    // Optimize
    QueryBuilder optimized = ESUtils.queryOptimize(rootQuery, false);

    // After optimization:
    // 1. middle1 and middle2 should clauses converted to filters
    // 2. filterOnly flattened into root
    // 3. leaf1 and leaf2 flattened
    // Result should be a single bool query with 2 term filters

    Assert.assertTrue(optimized instanceof BoolQueryBuilder);
    BoolQueryBuilder optimizedBool = (BoolQueryBuilder) optimized;

    Assert.assertEquals(2, optimizedBool.filter().size(), "Should have 2 filters at root level");

    // Both filters should be term queries, not bool queries
    for (QueryBuilder filter : optimizedBool.filter()) {
      Assert.assertTrue(
          filter instanceof TermQueryBuilder,
          "Filters should be term queries after full optimization");
    }
  }

  @Test
  public void testOptimizeDeepNestedAllFilters() {
    // Test the specific case from the example - deeply nested filters only
    BoolQueryBuilder level4 =
        QueryBuilders.boolQuery()
            .filter(QueryBuilders.termsQuery("source.urn", "urn:li:dataset:test"))
            .filter(QueryBuilders.termQuery("relationshipType", "DownstreamOf"))
            .filter(QueryBuilders.termsQuery("destination.entityType", "dataset"));

    BoolQueryBuilder level3 = QueryBuilders.boolQuery().filter(level4);

    BoolQueryBuilder level2 = QueryBuilders.boolQuery().filter(level3);

    BoolQueryBuilder level1 = QueryBuilders.boolQuery().filter(level2);

    // Optimize
    QueryBuilder optimized = ESUtils.queryOptimize(level1, false);

    // All nested bool queries with only filters should be flattened
    Assert.assertTrue(optimized instanceof BoolQueryBuilder);
    BoolQueryBuilder optimizedBool = (BoolQueryBuilder) optimized;

    // Should have 3 filters at the root level
    Assert.assertEquals(3, optimizedBool.filter().size());
    Assert.assertEquals(0, optimizedBool.must().size());
    Assert.assertEquals(0, optimizedBool.should().size());
    Assert.assertEquals(0, optimizedBool.mustNot().size());

    // All filters should be non-bool queries
    Assert.assertTrue(optimizedBool.filter().get(0) instanceof TermsQueryBuilder);
    Assert.assertTrue(optimizedBool.filter().get(1) instanceof TermQueryBuilder);
    Assert.assertTrue(optimizedBool.filter().get(2) instanceof TermsQueryBuilder);
  }

  @Test
  public void testOptimizeMixedShouldAndFilterNesting() {
    // Test a case with mixed should/filter nesting to ensure correct optimization

    BoolQueryBuilder innerFilter =
        QueryBuilders.boolQuery().filter(QueryBuilders.termQuery("status", "active"));

    BoolQueryBuilder shouldWithFilter =
        QueryBuilders.boolQuery().should(innerFilter).minimumShouldMatch("1");

    BoolQueryBuilder rootQuery =
        QueryBuilders.boolQuery()
            .must(QueryBuilders.termQuery("type", "dataset"))
            .filter(shouldWithFilter);

    // Optimize
    QueryBuilder optimized = ESUtils.queryOptimize(rootQuery, true);

    Assert.assertTrue(optimized instanceof BoolQueryBuilder);
    BoolQueryBuilder optimizedBool = (BoolQueryBuilder) optimized;

    // Should have the must clause preserved
    Assert.assertEquals(1, optimizedBool.must().size());

    // The should->filter optimization should have occurred
    // And the inner filter should be flattened
    Assert.assertEquals(1, optimizedBool.filter().size());

    // The filter should be the term query directly
    Assert.assertTrue(optimizedBool.filter().get(0) instanceof TermQueryBuilder);
    TermQueryBuilder filterTerm = (TermQueryBuilder) optimizedBool.filter().get(0);
    Assert.assertEquals("status", filterTerm.fieldName());
    Assert.assertEquals("active", filterTerm.value());
  }
}
