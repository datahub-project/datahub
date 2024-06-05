package com.linkedin.metadata.search.utils;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.schema.PathSpec;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.GraphRetriever;
import com.linkedin.metadata.models.annotation.SearchableAnnotation;
import com.linkedin.metadata.test.definition.expression.Query;
import com.linkedin.metadata.test.definition.literal.StringListLiteral;
import com.linkedin.metadata.test.definition.operator.Operand;
import com.linkedin.metadata.test.definition.operator.OperatorType;
import com.linkedin.metadata.test.definition.operator.Predicate;
import io.datahubproject.metadata.context.ActorContext;
import io.datahubproject.metadata.context.AuthorizerContext;
import io.datahubproject.metadata.context.ObjectMapperContext;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RetrieverContext;
import io.datahubproject.metadata.context.SearchContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.mockito.Mockito;
import org.opensearch.index.query.QueryBuilder;
import org.testng.Assert;
import org.testng.annotations.Test;


public class ESPredicateUtilsTest {

  private static final String FIELD_TO_EXPAND = "urn";
  private static final String TIMESERIES_FIELD_QUERY_PART_1 = "operation";
  private static final String TIMESERIES_FIELD = "lastOperationTime";

  @Test
  public void testBuildFilterQuery() {
    Query query = new Query(FIELD_TO_EXPAND);
    StringListLiteral stringListLiteral = new StringListLiteral(ImmutableList.of("value1"));
    Operand operand1 = new Operand(0, "operand1", query);
    Operand operand2 = new Operand(1, "operand2", stringListLiteral);
    Predicate equalTo = new Predicate(OperatorType.ANY_EQUALS, ImmutableList.of(operand1, operand2));
    Operand equalToOp = new Operand(0, "equalTo", equalTo);
    final Predicate predicate = new Predicate(OperatorType.AND, ImmutableList.of(equalToOp));
    Map<PathSpec, String> fieldPaths = new HashMap<>();
    fieldPaths.put(new PathSpec(FIELD_TO_EXPAND), FIELD_TO_EXPAND);
    Map<String, Set<SearchableAnnotation.FieldType>> fieldTypes = new HashMap<>();
    fieldTypes.put(FIELD_TO_EXPAND, Collections.singleton(SearchableAnnotation.FieldType.URN));

    OperationContext operationContext = TestOperationContexts
        .userContextNoSearchAuthorization(UrnUtils.getUrn("urn:li:corpuser:name"));


    // Ensure that the query is expanded!
    QueryBuilder result =
        ESPredicateUtils.buildFilterQuery(predicate, false, fieldPaths, fieldTypes, operationContext);
    String expected =
        "{\n"
            + "  \"bool\" : {\n"
            + "    \"should\" : [\n"
            + "      {\n"
            + "        \"bool\" : {\n"
            + "          \"should\" : [\n"
            + "            {\n"
            + "              \"terms\" : {\n"
            + "                \"urn\" : [\n"
            + "                  \"value1\"\n"
            + "                ],\n"
            + "                \"boost\" : 1.0\n"
            + "              }\n"
            + "            }\n"
            + "          ],\n"
            + "          \"adjust_pure_negative\" : true,\n"
            + "          \"minimum_should_match\" : \"1\",\n"
            + "          \"boost\" : 1.0\n"
            + "        }\n"
            + "      }\n"
            + "    ],\n"
            + "    \"adjust_pure_negative\" : true,\n"
            + "    \"minimum_should_match\" : \"1\",\n"
            + "    \"boost\" : 1.0\n"
            + "  }\n"
            + "}";
    Assert.assertEquals(result.toString(), expected);

    query = new Query(TIMESERIES_FIELD_QUERY_PART_1 + "." + TIMESERIES_FIELD);
    stringListLiteral = new StringListLiteral(ImmutableList.of("value1", "value2"));
    operand1 = new Operand(0, "operand1", query);
    operand2 = new Operand(1, "operand2", stringListLiteral);
    equalTo = new Predicate(OperatorType.ANY_EQUALS, ImmutableList.of(operand1, operand2));
    equalToOp = new Operand(0, "equalTo", equalTo);
    final Predicate predicate2 = new Predicate(OperatorType.AND, ImmutableList.of(equalToOp));
    Map<PathSpec, String> fieldPaths2 = new HashMap<>();
    fieldPaths2.put(new PathSpec(TIMESERIES_FIELD_QUERY_PART_1, TIMESERIES_FIELD), TIMESERIES_FIELD);
    Map<String, Set<SearchableAnnotation.FieldType>> fieldTypes2 = new HashMap<>();
    fieldTypes.put(TIMESERIES_FIELD, Collections.singleton(SearchableAnnotation.FieldType.DATETIME));

    // Ensure that the query is expanded without keyword.
    result = ESPredicateUtils.buildFilterQuery(predicate2, true, fieldPaths2, fieldTypes2, operationContext);
    expected =
        "{\n"
            + "  \"bool\" : {\n"
            + "    \"should\" : [\n"
            + "      {\n" + "        \"bool\" : {\n"
            + "          \"should\" : [\n"
            + "            {\n"
            + "              \"terms\" : {\n"
            + "                \"lastOperationTime\" : [\n"
            + "                  \"value1\",\n"
            + "                  \"value2\"\n"
            + "                ],\n"
            + "                \"boost\" : 1.0\n"
            + "              }\n"
            + "            }\n"
            + "          ],\n"
            + "          \"adjust_pure_negative\" : true,\n"
            + "          \"minimum_should_match\" : \"1\",\n"
            + "          \"boost\" : 1.0\n"
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
}
