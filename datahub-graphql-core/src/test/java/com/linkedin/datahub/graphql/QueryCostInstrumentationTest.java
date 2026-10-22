package com.linkedin.datahub.graphql;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import graphql.ExecutionInput;
import graphql.ExecutionResult;
import graphql.GraphQL;
import graphql.execution.values.InputInterceptor;
import graphql.execution.values.legacycoercing.LegacyCoercingInputInterceptor;
import graphql.schema.GraphQLSchema;
import graphql.schema.idl.RuntimeWiring;
import graphql.schema.idl.SchemaGenerator;
import graphql.schema.idl.SchemaParser;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class QueryCostInstrumentationTest {

  private static final GraphQLSchema SCHEMA =
      new SchemaGenerator()
          .makeExecutableSchema(
              new SchemaParser()
                  .parse(
                      """
                      type Query {
                        hello: String
                        node: Node
                        search(input: SearchInput): SearchResults
                        searchWithDefault(input: SearchInput = {count: 7}): SearchResults
                      }
                      type Mutation {
                        patch(input: [PatchInput!]): [String]
                      }
                      input SearchInput {
                        query: String
                        count: Int
                      }
                      input PatchInput {
                        urn: String
                      }
                      type Node {
                        name: String
                        child: Node
                      }
                      type SearchResults {
                        total: Int
                        searchResults: [SearchResult]
                      }
                      type SearchResult {
                        entity: Entity
                      }
                      type Entity {
                        urn: String
                      }
                      """),
              RuntimeWiring.newRuntimeWiring().build());

  private static final String LINEAGE_FRAGMENT = " fragment searchResultFields on Entity { urn }";

  /** search + searchResults + entity + urn. */
  private static final int LINEAGE_FIELDS = 4;

  private static final Pattern EXCEEDED =
      Pattern.compile("^maximum query (?:depth|complexity) exceeded (\\d+) > -1$");

  private static GraphQL graphQL(int maxDepth, int maxComplexity) {
    return GraphQL.newGraphQL(SCHEMA)
        .instrumentation(new QueryCostInstrumentation(maxDepth, maxComplexity))
        .preparsedDocumentProvider(
            new GraphqlPreparsedDocumentProvider(new GraphqlDocumentCache(1024 * 1024)))
        .build();
  }

  private static ExecutionResult execute(
      GraphQL graphQL, String query, Map<String, Object> variables) {
    Map<Object, Object> context = new HashMap<>();
    // Like GraphQLEngine: accepts a numeric string for an Int.
    context.put(InputInterceptor.class, LegacyCoercingInputInterceptor.migratesValues());
    return graphQL.execute(
        ExecutionInput.newExecutionInput()
            .query(query)
            .variables(variables)
            .graphQLContext(context)
            .build());
  }

  /**
   * Returns the value that a limit of -1 reports. Runs the query two times: the second run uses the
   * cached result, and must report the same value.
   */
  private static int reported(GraphQL probe, String query, Map<String, Object> variables) {
    Integer first = null;
    for (int run = 0; run < 2; run++) {
      ExecutionResult result = execute(probe, query, variables);
      assertEquals(result.getErrors().size(), 1, String.valueOf(result.getErrors()));
      Matcher matcher = EXCEEDED.matcher(result.getErrors().get(0).getMessage());
      assertTrue(matcher.matches(), result.getErrors().get(0).getMessage());
      int value = Integer.parseInt(matcher.group(1));
      if (first != null) {
        assertEquals(value, first.intValue());
      }
      first = value;
    }
    return first;
  }

  /** Report the value of each query. One per test, so that all queries in a test share a cache. */
  private GraphQL depthProbe;

  private GraphQL complexityProbe;

  @BeforeMethod
  public void setUp() {
    depthProbe = graphQL(-1, Integer.MAX_VALUE);
    complexityProbe = graphQL(Integer.MAX_VALUE, -1);
  }

  private int depth(String query) {
    return reported(depthProbe, query, Map.of());
  }

  private int complexity(String query, Map<String, Object> variables) {
    return reported(complexityProbe, query, variables);
  }

  private int complexity(String query) {
    return complexity(query, Map.of());
  }

  @Test
  public void testDepthCountsNestedFieldLevels() {
    assertEquals(depth("{ hello }"), 1);
    assertEquals(depth("{ node { child { name } } }"), 3);
  }

  @Test
  public void testFragmentsDoNotAddDepth() {
    assertEquals(depth("{ node { ...F } } fragment F on Node { child { name } }"), 3);
    assertEquals(depth("{ node { ... on Node { child { name } } } }"), 3);
  }

  @Test
  public void testComplexityCountsFieldsButNotTypename() {
    assertEquals(complexity("{ node { name child { name } } }"), 4);
    assertEquals(complexity("{ __typename node { __typename name } }"), 2);
  }

  @Test
  public void testFragmentUsedManyTimesCountsOnce() {
    // node + other + (name + child + name) from the fragment, one time.
    assertEquals(
        complexity(
            "{ node { ...F } other: node { ...F } } fragment F on Node { name child { name } }"),
        5);
  }

  @Test
  public void testSkipAndIncludeRemoveFields() {
    assertEquals(complexity("{ node @skip(if: true) { name } hello }"), 1);
    assertEquals(complexity("{ node @include(if: false) { name } hello }"), 1);

    String query = "query q($skip: Boolean!) { node @skip(if: $skip) { name } hello }";
    // The same query with different Boolean values must not share a cached result.
    assertEquals(complexity(query, Map.of("skip", true)), 1);
    assertEquals(complexity(query, Map.of("skip", false)), 3);
    assertEquals(complexity(query, Map.of("skip", true)), 1);
  }

  @Test
  public void testLineageFieldAddsTwiceItsCount() {
    String literal =
        "{ search(input: {count: 5}) { searchResults { entity { ...searchResultFields } } } }"
            + LINEAGE_FRAGMENT;
    assertEquals(complexity(literal), LINEAGE_FIELDS + 10);

    String variable =
        "query q($count: Int = 5) { search(input: {count: $count}) { searchResults { entity {"
            + " ...searchResultFields } } } }"
            + LINEAGE_FRAGMENT;
    assertEquals(complexity(variable, Map.of()), LINEAGE_FIELDS + 10);
    assertEquals(complexity(variable, Map.of("count", 20)), LINEAGE_FIELDS + 40);
    assertEquals(complexity(variable, Map.of("count", "20")), LINEAGE_FIELDS + 40);
    assertEquals(complexity(variable, Map.of("count", 1)), LINEAGE_FIELDS);
  }

  @Test
  public void testLineageCountFromSchemaDefault() {
    assertEquals(
        complexity(
            "{ searchWithDefault { searchResults { entity { ...searchResultFields } } } }"
                + LINEAGE_FRAGMENT),
        LINEAGE_FIELDS + 14);
  }

  @Test
  public void testCountWithoutLineageShapeAddsNothing() {
    assertEquals(complexity("{ search(input: {count: 5}) { total } }"), 2);
  }

  @Test
  public void testNullCountAddsNothing() {
    assertEquals(
        complexity(
            "{ search(input: {count: null}) { searchResults { entity { ...searchResultFields } } }"
                + " }"
                + LINEAGE_FRAGMENT),
        LINEAGE_FIELDS);
  }

  @Test
  public void testListInputAddsItsSize() {
    assertEquals(complexity("mutation { patch(input: [{urn: \"a\"}, {urn: \"b\"}]) }"), 3);
    // A single object for a list argument becomes a list of one.
    assertEquals(complexity("mutation { patch(input: {urn: \"a\"}) }"), 2);

    String variable = "mutation m($input: [PatchInput!]) { patch(input: $input) }";
    assertEquals(complexity(variable, Map.of("input", List.of(Map.of(), Map.of(), Map.of()))), 4);
  }

  @Test
  public void testVeryLargeCountStopsAtMaxInt() {
    assertEquals(
        complexity(
            "{ search(input: {count: 1100000000}) { searchResults { entity {"
                + " ...searchResultFields } } } }"
                + LINEAGE_FRAGMENT),
        Integer.MAX_VALUE);
  }

  @Test
  public void testChecksTheOperationThatRuns() {
    String query = "query Deep { node { child { name } } } query Shallow { hello }";
    GraphQL graphQL = graphQL(2, Integer.MAX_VALUE);

    assertNull(executeOperation(graphQL, query, "Deep").getData());
    assertTrue(executeOperation(graphQL, query, "Shallow").getErrors().isEmpty());
    // An empty name runs the first operation.
    assertNull(executeOperation(graphQL, query, "").getData());
  }

  @Test
  public void testQueryWithinLimitsRuns() {
    ExecutionResult result = execute(graphQL(3, 4), "{ node { name child { name } } }", Map.of());

    assertTrue(result.getErrors().isEmpty(), String.valueOf(result.getErrors()));
    assertNotNull(result.getData());
  }

  @Test
  public void testDepthIsCheckedBeforeComplexity() {
    ExecutionResult result = execute(graphQL(1, 1), "{ node { name } }", Map.of());

    assertNull(result.getData());
    assertEquals(result.getErrors().size(), 1);
    assertEquals(result.getErrors().get(0).getMessage(), "maximum query depth exceeded 2 > 1");
  }

  @Test
  public void testWorksWithoutDocumentCache() {
    GraphQL graphQL =
        GraphQL.newGraphQL(SCHEMA).instrumentation(new QueryCostInstrumentation(10, 1)).build();

    ExecutionResult result = execute(graphQL, "{ node { name } }", Map.of());

    assertEquals(result.getErrors().get(0).getMessage(), "maximum query complexity exceeded 2 > 1");
  }

  private static ExecutionResult executeOperation(
      GraphQL graphQL, String query, String operationName) {
    return graphQL.execute(
        ExecutionInput.newExecutionInput().query(query).operationName(operationName).build());
  }
}
