package com.linkedin.metadata.search.utils;

import static com.linkedin.metadata.test.TestDefinitionParserTest.*;
import static io.datahubproject.test.metadata.context.TestOperationContexts.TEST_USER_AUTH;
import static org.testng.Assert.*;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.NumericNode;
import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.schema.PathSpec;
import com.linkedin.metadata.config.search.EntityIndexConfiguration;
import com.linkedin.metadata.models.annotation.SearchableAnnotation;
import com.linkedin.metadata.query.SearchFlags;
import com.linkedin.metadata.search.elasticsearch.query.filter.QueryFilterRewriteChain;
import com.linkedin.metadata.test.definition.TestDefinition;
import com.linkedin.metadata.test.definition.TestDefinitionParser;
import com.linkedin.metadata.test.definition.expression.Query;
import com.linkedin.metadata.test.definition.literal.DateLiteral;
import com.linkedin.metadata.test.definition.literal.StringListLiteral;
import com.linkedin.metadata.test.definition.operator.Operand;
import com.linkedin.metadata.test.definition.operator.OperatorType;
import com.linkedin.metadata.test.definition.operator.Predicate;
import com.linkedin.metadata.test.eval.PredicateEvaluator;
import com.linkedin.metadata.utils.elasticsearch.IndexConventionImpl;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.SearchContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.opensearch.index.query.QueryBuilder;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ESPredicateUtilsTest {

  private static final String FIELD_TO_EXPAND = "urn";
  private static final String TIMESERIES_FIELD_QUERY_PART_1 = "operation";
  private static final String TIMESERIES_FIELD = "lastOperationTime";
  private static final String DATASET_PROPERTIES = "datasetProperties";
  private static final String EDITABLE_DATASET_PROPERTIES = "editableDatasetProperties";
  private static final String NAME = "name";
  private static final String DESCRIPTION = "description";
  private static final String EDITED_DESCRIPTION = "editedDescription";

  private static final TestDefinitionParser PARSER =
      new TestDefinitionParser(PredicateEvaluator.getInstance());

  private static final Urn TEST_URN = UrnUtils.getUrn("urn:li:test:test");
  private static final OperationContext OPERATION_CONTEXT =
      TestOperationContexts.userContextNoSearchAuthorization(
              UrnUtils.getUrn("urn:li:corpuser:name"))
          .toBuilder()
          .searchContext(
              SearchContext.builder()
                  .indexConvention(IndexConventionImpl.noPrefix("", new EntityIndexConfiguration()))
                  .searchableFieldTypes(Collections.emptyMap())
                  .searchableFieldPaths(Collections.emptyMap())
                  .searchFlags(new SearchFlags().setFilterNonLatestVersions(true))
                  .build())
          .build(TEST_USER_AUTH, true);

  @Test
  public void testBuildFilterQuery() throws JsonProcessingException {
    Query query = new Query(FIELD_TO_EXPAND);
    StringListLiteral stringListLiteral = new StringListLiteral(ImmutableList.of("value1"));
    Operand operand1 = new Operand(0, "operand1", query);
    Operand operand2 = new Operand(1, "operand2", stringListLiteral);
    Predicate equalTo =
        new Predicate(OperatorType.ANY_EQUALS, ImmutableList.of(operand1, operand2));
    Operand equalToOp = new Operand(0, "equalTo", equalTo);
    final Predicate predicate = new Predicate(OperatorType.AND, ImmutableList.of(equalToOp));
    Map<PathSpec, String> fieldPaths = new HashMap<>();
    fieldPaths.put(new PathSpec(FIELD_TO_EXPAND), FIELD_TO_EXPAND);
    Map<String, Set<SearchableAnnotation.FieldType>> fieldTypes = new HashMap<>();
    fieldTypes.put(FIELD_TO_EXPAND, Collections.singleton(SearchableAnnotation.FieldType.URN));

    // Ensure that the query is expanded!
    QueryBuilder result =
        ESPredicateUtils.buildFilterQuery(
            predicate,
            false,
            fieldPaths,
            fieldTypes,
            OPERATION_CONTEXT,
            QueryFilterRewriteChain.EMPTY);
    String expected =
        "{\n"
            + "  \"bool\" : {\n"
            + "    \"must\" : [\n"
            + "      {\n"
            + "        \"bool\" : {\n"
            + "          \"should\" : [\n"
            + "            {\n"
            + "              \"bool\" : {\n"
            + "                \"filter\" : [\n"
            + "                  {\n"
            + "                    \"terms\" : {\n"
            + "                      \"isLatest.keyword\" : [\n"
            + "                        \"true\"\n"
            + "                      ],\n"
            + "                      \"boost\" : 1.0,\n"
            + "                      \"_name\" : \"isLatest\"\n"
            + "                    }\n"
            + "                  }\n"
            + "                ],\n"
            + "                \"adjust_pure_negative\" : true,\n"
            + "                \"boost\" : 1.0\n"
            + "              }\n"
            + "            },\n"
            + "            {\n"
            + "              \"bool\" : {\n"
            + "                \"must_not\" : [\n"
            + "                  {\n"
            + "                    \"bool\" : {\n"
            + "                      \"must\" : [\n"
            + "                        {\n"
            + "                          \"exists\" : {\n"
            + "                            \"field\" : \"isLatest\",\n"
            + "                            \"boost\" : 1.0\n"
            + "                          }\n"
            + "                        }\n"
            + "                      ],\n"
            + "                      \"adjust_pure_negative\" : true,\n"
            + "                      \"boost\" : 1.0,\n"
            + "                      \"_name\" : \"isLatest\"\n"
            + "                    }\n"
            + "                  }\n"
            + "                ],\n"
            + "                \"adjust_pure_negative\" : true,\n"
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
    stringListLiteral = new StringListLiteral(ImmutableList.of("123456789", "987654321"));
    operand1 = new Operand(0, "operand1", query);
    operand2 = new Operand(1, "operand2", stringListLiteral);
    equalTo = new Predicate(OperatorType.ANY_EQUALS, ImmutableList.of(operand1, operand2));
    equalToOp = new Operand(0, "equalTo", equalTo);
    final Predicate predicate2 = new Predicate(OperatorType.AND, ImmutableList.of(equalToOp));
    Map<PathSpec, String> fieldPaths2 = new HashMap<>();
    fieldPaths2.put(
        new PathSpec(TIMESERIES_FIELD_QUERY_PART_1, TIMESERIES_FIELD), TIMESERIES_FIELD);
    Map<String, Set<SearchableAnnotation.FieldType>> fieldTypes2 = new HashMap<>();
    fieldTypes2.put(
        TIMESERIES_FIELD, Collections.singleton(SearchableAnnotation.FieldType.DATETIME));

    // Ensure that the query is expanded without keyword.
    result =
        ESPredicateUtils.buildFilterQuery(
            predicate2,
            true,
            fieldPaths2,
            fieldTypes2,
            OPERATION_CONTEXT,
            QueryFilterRewriteChain.EMPTY);
    expected =
        "{\n"
            + "  \"bool\" : {\n"
            + "    \"must\" : [\n"
            + "      {\n"
            + "        \"bool\" : {\n"
            + "          \"should\" : [\n"
            + "            {\n"
            + "              \"bool\" : {\n"
            + "                \"filter\" : [\n"
            + "                  {\n"
            + "                    \"terms\" : {\n"
            + "                      \"isLatest.keyword\" : [\n"
            + "                        \"true\"\n"
            + "                      ],\n"
            + "                      \"boost\" : 1.0,\n"
            + "                      \"_name\" : \"isLatest\"\n"
            + "                    }\n"
            + "                  }\n"
            + "                ],\n"
            + "                \"adjust_pure_negative\" : true,\n"
            + "                \"boost\" : 1.0\n"
            + "              }\n"
            + "            },\n"
            + "            {\n"
            + "              \"bool\" : {\n"
            + "                \"must_not\" : [\n"
            + "                  {\n"
            + "                    \"bool\" : {\n"
            + "                      \"must\" : [\n"
            + "                        {\n"
            + "                          \"exists\" : {\n"
            + "                            \"field\" : \"isLatest\",\n"
            + "                            \"boost\" : 1.0\n"
            + "                          }\n"
            + "                        }\n"
            + "                      ],\n"
            + "                      \"adjust_pure_negative\" : true,\n"
            + "                      \"boost\" : 1.0,\n"
            + "                      \"_name\" : \"isLatest\"\n"
            + "                    }\n"
            + "                  }\n"
            + "                ],\n"
            + "                \"adjust_pure_negative\" : true,\n"
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
            + "    \"should\" : [\n"
            + "      {\n"
            + "        \"bool\" : {\n"
            + "          \"should\" : [\n"
            + "            {\n"
            + "              \"terms\" : {\n"
            + "                \"lastOperationTime\" : [\n"
            + "                  123456789,\n"
            + "                  987654321\n"
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
    long millis = System.currentTimeMillis();
    DateLiteral dateLiteral = new DateLiteral("-7d");
    query = new Query(TIMESERIES_FIELD_QUERY_PART_1 + "." + TIMESERIES_FIELD);
    operand1 = new Operand(0, "operand1", query);
    operand2 = new Operand(1, "operand2", dateLiteral);
    equalTo = new Predicate(OperatorType.ANY_EQUALS, ImmutableList.of(operand1, operand2));
    equalToOp = new Operand(0, "equalTo", equalTo);
    final Predicate predicate3 = new Predicate(OperatorType.AND, ImmutableList.of(equalToOp));
    Map<PathSpec, String> fieldPaths3 = new HashMap<>();
    fieldPaths3.put(
        new PathSpec(TIMESERIES_FIELD_QUERY_PART_1, TIMESERIES_FIELD), TIMESERIES_FIELD);
    Map<String, Set<SearchableAnnotation.FieldType>> fieldTypes3 = new HashMap<>();
    fieldTypes3.put(
        TIMESERIES_FIELD, Collections.singleton(SearchableAnnotation.FieldType.DATETIME));

    // Ensure that the query is expanded without keyword.
    result =
        ESPredicateUtils.buildFilterQuery(
            predicate3,
            true,
            fieldPaths3,
            fieldTypes3,
            OPERATION_CONTEXT,
            QueryFilterRewriteChain.EMPTY);
    JsonNode jsonNode = new ObjectMapper().readTree(result.toString());
    long millis7DaysAgo = millis - 7 * 24 * 60 * 60 * 1000;
    long millis6DaysAgo = millis - 6 * 24 * 60 * 60 * 1000;
    NumericNode resolvedDate =
        (NumericNode)
            jsonNode
                .get("bool")
                .get("should")
                .get(0)
                .get("bool")
                .get("should")
                .get(0)
                .get("terms")
                .get("lastOperationTime")
                .get(0);
    assertTrue(resolvedDate.asLong() < millis6DaysAgo);
    assertTrue(
        resolvedDate.asLong() >= millis7DaysAgo); // computedMillis is between 6 and 7 days ago
  }

  @Test
  public void testComplex() throws Exception {
    String jsonTest = loadTest("valid_test_complex.yaml");
    TestDefinition def = PARSER.deserialize(TEST_URN, jsonTest);
    Predicate predicate =
        Predicate.of(
            OperatorType.AND, ImmutableList.of(def.getRules(), def.getOn().getConditions()));
    Map<PathSpec, String> fieldPaths = new HashMap<>();
    fieldPaths.put(new PathSpec(DATASET_PROPERTIES, NAME), NAME);
    fieldPaths.put(new PathSpec(DATASET_PROPERTIES, DESCRIPTION), DESCRIPTION);
    fieldPaths.put(new PathSpec(EDITABLE_DATASET_PROPERTIES, DESCRIPTION), EDITED_DESCRIPTION);
    Map<String, Set<SearchableAnnotation.FieldType>> fieldTypes = new HashMap<>();
    fieldTypes.put(NAME, Collections.singleton(SearchableAnnotation.FieldType.TEXT));
    fieldTypes.put(DESCRIPTION, Collections.singleton(SearchableAnnotation.FieldType.TEXT));
    fieldTypes.put(EDITED_DESCRIPTION, Collections.singleton(SearchableAnnotation.FieldType.TEXT));
    QueryBuilder result =
        ESPredicateUtils.buildFilterQuery(
            predicate,
            false,
            fieldPaths,
            fieldTypes,
            OPERATION_CONTEXT,
            QueryFilterRewriteChain.EMPTY);
    String expected =
        "{\n"
            + "  \"bool\" : {\n"
            + "    \"must\" : [\n"
            + "      {\n"
            + "        \"bool\" : {\n"
            + "          \"should\" : [\n"
            + "            {\n"
            + "              \"bool\" : {\n"
            + "                \"filter\" : [\n"
            + "                  {\n"
            + "                    \"terms\" : {\n"
            + "                      \"isLatest.keyword\" : [\n"
            + "                        \"true\"\n"
            + "                      ],\n"
            + "                      \"boost\" : 1.0,\n"
            + "                      \"_name\" : \"isLatest\"\n"
            + "                    }\n"
            + "                  }\n"
            + "                ],\n"
            + "                \"adjust_pure_negative\" : true,\n"
            + "                \"boost\" : 1.0\n"
            + "              }\n"
            + "            },\n"
            + "            {\n"
            + "              \"bool\" : {\n"
            + "                \"must_not\" : [\n"
            + "                  {\n"
            + "                    \"bool\" : {\n"
            + "                      \"must\" : [\n"
            + "                        {\n"
            + "                          \"exists\" : {\n"
            + "                            \"field\" : \"isLatest\",\n"
            + "                            \"boost\" : 1.0\n"
            + "                          }\n"
            + "                        }\n"
            + "                      ],\n"
            + "                      \"adjust_pure_negative\" : true,\n"
            + "                      \"boost\" : 1.0,\n"
            + "                      \"_name\" : \"isLatest\"\n"
            + "                    }\n"
            + "                  }\n"
            + "                ],\n"
            + "                \"adjust_pure_negative\" : true,\n"
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
            + "    \"should\" : [\n"
            + "      {\n"
            + "        \"bool\" : {\n"
            + "          \"should\" : [\n"
            + "            {\n"
            + "              \"bool\" : {\n"
            + "                \"must_not\" : [\n"
            + "                  {\n"
            + "                    \"bool\" : {\n"
            + "                      \"should\" : [\n"
            + "                        {\n"
            + "                          \"exists\" : {\n"
            + "                            \"field\" : \"editedDescription.keyword\",\n"
            + "                            \"boost\" : 1.0\n"
            + "                          }\n"
            + "                        }\n"
            + "                      ],\n"
            + "                      \"adjust_pure_negative\" : true,\n"
            + "                      \"minimum_should_match\" : \"1\",\n"
            + "                      \"boost\" : 1.0\n"
            + "                    }\n"
            + "                  }\n"
            + "                ],\n"
            + "                \"adjust_pure_negative\" : true,\n"
            + "                \"boost\" : 1.0\n"
            + "              }\n"
            + "            },\n"
            + "            {\n"
            + "              \"bool\" : {\n"
            + "                \"should\" : [\n"
            + "                  {\n"
            + "                    \"bool\" : {\n"
            + "                      \"should\" : [\n"
            + "                        {\n"
            + "                          \"terms\" : {\n"
            + "                            \"editedDescription.keyword\" : [\n"
            + "                              \"required field option 1\"\n"
            + "                            ],\n"
            + "                            \"boost\" : 1.0\n"
            + "                          }\n"
            + "                        }\n"
            + "                      ],\n"
            + "                      \"adjust_pure_negative\" : true,\n"
            + "                      \"minimum_should_match\" : \"1\",\n"
            + "                      \"boost\" : 1.0\n"
            + "                    }\n"
            + "                  },\n"
            + "                  {\n"
            + "                    \"bool\" : {\n"
            + "                      \"should\" : [\n"
            + "                        {\n"
            + "                          \"terms\" : {\n"
            + "                            \"description.keyword\" : [\n"
            + "                              \"required field option 2\"\n"
            + "                            ],\n"
            + "                            \"boost\" : 1.0\n"
            + "                          }\n"
            + "                        },\n"
            + "                        {\n"
            + "                          \"terms\" : {\n"
            + "                            \"editedDescription.keyword\" : [\n"
            + "                              \"required field option 2\"\n"
            + "                            ],\n"
            + "                            \"boost\" : 1.0\n"
            + "                          }\n"
            + "                        }\n"
            + "                      ],\n"
            + "                      \"adjust_pure_negative\" : true,\n"
            + "                      \"minimum_should_match\" : \"1\",\n"
            + "                      \"boost\" : 1.0\n"
            + "                    }\n"
            + "                  }\n"
            + "                ],\n"
            + "                \"adjust_pure_negative\" : true,\n"
            + "                \"minimum_should_match\" : \"1\",\n"
            + "                \"boost\" : 1.0\n"
            + "              }\n"
            + "            },\n"
            + "            {\n"
            + "              \"bool\" : {\n"
            + "                \"should\" : [\n"
            + "                  {\n"
            + "                    \"bool\" : {\n"
            + "                      \"should\" : [\n"
            + "                        {\n"
            + "                          \"bool\" : {\n"
            + "                            \"should\" : [\n"
            + "                              {\n"
            + "                                \"bool\" : {\n"
            + "                                  \"must_not\" : [\n"
            + "                                    {\n"
            + "                                      \"bool\" : {\n"
            + "                                        \"should\" : [\n"
            + "                                          {\n"
            + "                                            \"bool\" : {\n"
            + "                                              \"should\" : [\n"
            + "                                                {\n"
            + "                                                  \"terms\" : {\n"
            + "                                                    \"description.keyword\" : [\n"
            + "                                                      \"required field option 2\"\n"
            + "                                                    ],\n"
            + "                                                    \"boost\" : 1.0\n"
            + "                                                  }\n"
            + "                                                },\n"
            + "                                                {\n"
            + "                                                  \"terms\" : {\n"
            + "                                                    \"editedDescription.keyword\" : [\n"
            + "                                                      \"required field option 2\"\n"
            + "                                                    ],\n"
            + "                                                    \"boost\" : 1.0\n"
            + "                                                  }\n"
            + "                                                }\n"
            + "                                              ],\n"
            + "                                              \"adjust_pure_negative\" : true,\n"
            + "                                              \"minimum_should_match\" : \"1\",\n"
            + "                                              \"boost\" : 1.0\n"
            + "                                            }\n"
            + "                                          }\n"
            + "                                        ],\n"
            + "                                        \"adjust_pure_negative\" : true,\n"
            + "                                        \"minimum_should_match\" : \"1\",\n"
            + "                                        \"boost\" : 1.0\n"
            + "                                      }\n"
            + "                                    }\n"
            + "                                  ],\n"
            + "                                  \"adjust_pure_negative\" : true,\n"
            + "                                  \"boost\" : 1.0\n"
            + "                                }\n"
            + "                              },\n"
            + "                              {\n"
            + "                                \"bool\" : {\n"
            + "                                  \"should\" : [\n"
            + "                                    {\n"
            + "                                      \"bool\" : {\n"
            + "                                        \"should\" : [\n"
            + "                                          {\n"
            + "                                            \"term\" : {\n"
            + "                                              \"description.keyword\" : {\n"
            + "                                                \"value\" : false,\n"
            + "                                                \"boost\" : 1.0\n"
            + "                                              }\n"
            + "                                            }\n"
            + "                                          },\n"
            + "                                          {\n"
            + "                                            \"term\" : {\n"
            + "                                              \"editedDescription.keyword\" : {\n"
            + "                                                \"value\" : false,\n"
            + "                                                \"boost\" : 1.0\n"
            + "                                              }\n"
            + "                                            }\n"
            + "                                          }\n"
            + "                                        ],\n"
            + "                                        \"adjust_pure_negative\" : true,\n"
            + "                                        \"minimum_should_match\" : \"1\",\n"
            + "                                        \"boost\" : 1.0\n"
            + "                                      }\n"
            + "                                    },\n"
            + "                                    {\n"
            + "                                      \"bool\" : {\n"
            + "                                        \"should\" : [\n"
            + "                                          {\n"
            + "                                            \"term\" : {\n"
            + "                                              \"description.keyword\" : {\n"
            + "                                                \"value\" : true,\n"
            + "                                                \"boost\" : 1.0\n"
            + "                                              }\n"
            + "                                            }\n"
            + "                                          },\n"
            + "                                          {\n"
            + "                                            \"term\" : {\n"
            + "                                              \"editedDescription.keyword\" : {\n"
            + "                                                \"value\" : true,\n"
            + "                                                \"boost\" : 1.0\n"
            + "                                              }\n"
            + "                                            }\n"
            + "                                          }\n"
            + "                                        ],\n"
            + "                                        \"adjust_pure_negative\" : true,\n"
            + "                                        \"minimum_should_match\" : \"1\",\n"
            + "                                        \"boost\" : 1.0\n"
            + "                                      }\n"
            + "                                    }\n"
            + "                                  ],\n"
            + "                                  \"adjust_pure_negative\" : true,\n"
            + "                                  \"minimum_should_match\" : \"1\",\n"
            + "                                  \"boost\" : 1.0\n"
            + "                                }\n"
            + "                              }\n"
            + "                            ],\n"
            + "                            \"adjust_pure_negative\" : true,\n"
            + "                            \"minimum_should_match\" : \"2\",\n"
            + "                            \"boost\" : 1.0\n"
            + "                          }\n"
            + "                        },\n"
            + "                        {\n"
            + "                          \"bool\" : {\n"
            + "                            \"should\" : [\n"
            + "                              {\n"
            + "                                \"terms\" : {\n"
            + "                                  \"description.keyword\" : [\n"
            + "                                    \"required field option 2\"\n"
            + "                                  ],\n"
            + "                                  \"boost\" : 1.0\n"
            + "                                }\n"
            + "                              },\n"
            + "                              {\n"
            + "                                \"terms\" : {\n"
            + "                                  \"editedDescription.keyword\" : [\n"
            + "                                    \"required field option 2\"\n"
            + "                                  ],\n"
            + "                                  \"boost\" : 1.0\n"
            + "                                }\n"
            + "                              }\n"
            + "                            ],\n"
            + "                            \"adjust_pure_negative\" : true,\n"
            + "                            \"minimum_should_match\" : \"1\",\n"
            + "                            \"boost\" : 1.0\n"
            + "                          }\n"
            + "                        }\n"
            + "                      ],\n"
            + "                      \"adjust_pure_negative\" : true,\n"
            + "                      \"minimum_should_match\" : \"1\",\n"
            + "                      \"boost\" : 1.0\n"
            + "                    }\n"
            + "                  },\n"
            + "                  {\n"
            + "                    \"bool\" : {\n"
            + "                      \"must_not\" : [\n"
            + "                        {\n"
            + "                          \"bool\" : {\n"
            + "                            \"should\" : [\n"
            + "                              {\n"
            + "                                \"bool\" : {\n"
            + "                                  \"should\" : [\n"
            + "                                    {\n"
            + "                                      \"terms\" : {\n"
            + "                                        \"description.keyword\" : [\n"
            + "                                          \"required field option 2\"\n"
            + "                                        ],\n"
            + "                                        \"boost\" : 1.0\n"
            + "                                      }\n"
            + "                                    },\n"
            + "                                    {\n"
            + "                                      \"terms\" : {\n"
            + "                                        \"editedDescription.keyword\" : [\n"
            + "                                          \"required field option 2\"\n"
            + "                                        ],\n"
            + "                                        \"boost\" : 1.0\n"
            + "                                      }\n"
            + "                                    }\n"
            + "                                  ],\n"
            + "                                  \"adjust_pure_negative\" : true,\n"
            + "                                  \"minimum_should_match\" : \"1\",\n"
            + "                                  \"boost\" : 1.0\n"
            + "                                }\n"
            + "                              }\n"
            + "                            ],\n"
            + "                            \"adjust_pure_negative\" : true,\n"
            + "                            \"minimum_should_match\" : \"1\",\n"
            + "                            \"boost\" : 1.0\n"
            + "                          }\n"
            + "                        }\n"
            + "                      ],\n"
            + "                      \"adjust_pure_negative\" : true,\n"
            + "                      \"boost\" : 1.0\n"
            + "                    }\n"
            + "                  }\n"
            + "                ],\n"
            + "                \"adjust_pure_negative\" : true,\n"
            + "                \"minimum_should_match\" : \"2\",\n"
            + "                \"boost\" : 1.0\n"
            + "              }\n"
            + "            }\n"
            + "          ],\n"
            + "          \"adjust_pure_negative\" : true,\n"
            + "          \"minimum_should_match\" : \"3\",\n"
            + "          \"boost\" : 1.0\n"
            + "        }\n"
            + "      },\n"
            + "      {\n"
            + "        \"bool\" : {\n"
            + "          \"must_not\" : [\n"
            + "            {\n"
            + "              \"bool\" : {\n"
            + "                \"should\" : [\n"
            + "                  {\n"
            + "                    \"bool\" : {\n"
            + "                      \"should\" : [\n"
            + "                        {\n"
            + "                          \"prefix\" : {\n"
            + "                            \"name.keyword\" : {\n"
            + "                              \"value\" : \"special_prefix\",\n"
            + "                              \"boost\" : 1.0\n"
            + "                            }\n"
            + "                          }\n"
            + "                        }\n"
            + "                      ],\n"
            + "                      \"adjust_pure_negative\" : true,\n"
            + "                      \"minimum_should_match\" : \"1\",\n"
            + "                      \"boost\" : 1.0\n"
            + "                    }\n"
            + "                  },\n"
            + "                  {\n"
            + "                    \"bool\" : {\n"
            + "                      \"should\" : [\n"
            + "                        {\n"
            + "                          \"regexp\" : {\n"
            + "                            \"name.keyword\" : {\n"
            + "                              \"value\" : \".*exclude.*\",\n"
            + "                              \"flags_value\" : 255,\n"
            + "                              \"max_determinized_states\" : 10000,\n"
            + "                              \"boost\" : 1.0\n"
            + "                            }\n"
            + "                          }\n"
            + "                        }\n"
            + "                      ],\n"
            + "                      \"adjust_pure_negative\" : true,\n"
            + "                      \"minimum_should_match\" : \"1\",\n"
            + "                      \"boost\" : 1.0\n"
            + "                    }\n"
            + "                  },\n"
            + "                  {\n"
            + "                    \"bool\" : {\n"
            + "                      \"should\" : [\n"
            + "                        {\n"
            + "                          \"bool\" : {\n"
            + "                            \"should\" : [\n"
            + "                              {\n"
            + "                                \"exists\" : {\n"
            + "                                  \"field\" : \"editedDescription.keyword\",\n"
            + "                                  \"boost\" : 1.0\n"
            + "                                }\n"
            + "                              }\n"
            + "                            ],\n"
            + "                            \"adjust_pure_negative\" : true,\n"
            + "                            \"minimum_should_match\" : \"1\",\n"
            + "                            \"boost\" : 1.0\n"
            + "                          }\n"
            + "                        },\n"
            + "                        {\n"
            + "                          \"bool\" : {\n"
            + "                            \"should\" : [\n"
            + "                              {\n"
            + "                                \"bool\" : {\n"
            + "                                  \"must_not\" : [\n"
            + "                                    {\n"
            + "                                      \"bool\" : {\n"
            + "                                        \"should\" : [\n"
            + "                                          {\n"
            + "                                            \"prefix\" : {\n"
            + "                                              \"description.keyword\" : {\n"
            + "                                                \"value\" : \"special_prefix\",\n"
            + "                                                \"boost\" : 1.0\n"
            + "                                              }\n"
            + "                                            }\n"
            + "                                          },\n"
            + "                                          {\n"
            + "                                            \"prefix\" : {\n"
            + "                                              \"editedDescription.keyword\" : {\n"
            + "                                                \"value\" : \"special_prefix\",\n"
            + "                                                \"boost\" : 1.0\n"
            + "                                              }\n"
            + "                                            }\n"
            + "                                          }\n"
            + "                                        ],\n"
            + "                                        \"adjust_pure_negative\" : true,\n"
            + "                                        \"minimum_should_match\" : \"1\",\n"
            + "                                        \"boost\" : 1.0\n"
            + "                                      }\n"
            + "                                    }\n"
            + "                                  ],\n"
            + "                                  \"adjust_pure_negative\" : true,\n"
            + "                                  \"boost\" : 1.0\n"
            + "                                }\n"
            + "                              }\n"
            + "                            ],\n"
            + "                            \"adjust_pure_negative\" : true,\n"
            + "                            \"minimum_should_match\" : \"1\",\n"
            + "                            \"boost\" : 1.0\n"
            + "                          }\n"
            + "                        }\n"
            + "                      ],\n"
            + "                      \"adjust_pure_negative\" : true,\n"
            + "                      \"minimum_should_match\" : \"1\",\n"
            + "                      \"boost\" : 1.0\n"
            + "                    }\n"
            + "                  }\n"
            + "                ],\n"
            + "                \"adjust_pure_negative\" : true,\n"
            + "                \"minimum_should_match\" : \"3\",\n"
            + "                \"boost\" : 1.0\n"
            + "              }\n"
            + "            }\n"
            + "          ],\n"
            + "          \"adjust_pure_negative\" : true,\n"
            + "          \"boost\" : 1.0\n"
            + "        }\n"
            + "      }\n"
            + "    ],\n"
            + "    \"adjust_pure_negative\" : true,\n"
            + "    \"minimum_should_match\" : \"2\",\n"
            + "    \"boost\" : 1.0\n"
            + "  }\n"
            + "}";
    assertEquals(result.toString(), expected);
  }
}
