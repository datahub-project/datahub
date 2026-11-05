package com.linkedin.datahub.graphql.instrumentation;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.datahub.graphql.QueryContext;
import graphql.ExecutionInput;
import graphql.ExecutionResult;
import graphql.GraphQL;
import graphql.schema.GraphQLSchema;
import graphql.schema.idl.RuntimeWiring;
import graphql.schema.idl.SchemaGenerator;
import graphql.schema.idl.SchemaParser;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.List;
import java.util.Map;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class DeprecatedFieldUsageInstrumentationTest {

  private static final String SDL =
      """
      type Query {
        current: String
        legacy: String @deprecated(reason: "Use current.")
        items: [Item!]
      }
      type Item {
        name: String
        oldName: String @deprecated(reason: "Use name.")
      }
      """;

  private SimpleMeterRegistry meterRegistry;
  private GraphQL graphQL;

  @BeforeMethod
  public void setUp() {
    meterRegistry = new SimpleMeterRegistry();
    final RuntimeWiring wiring =
        RuntimeWiring.newRuntimeWiring()
            .type(
                "Query",
                builder ->
                    builder
                        .dataFetcher("current", env -> "now")
                        .dataFetcher("legacy", env -> "then")
                        .dataFetcher(
                            "items",
                            env ->
                                List.of(
                                    Map.of("name", "a", "oldName", "a"),
                                    Map.of("name", "b", "oldName", "b"))))
            .build();
    final GraphQLSchema schema =
        new SchemaGenerator().makeExecutableSchema(new SchemaParser().parse(SDL), wiring);
    graphQL =
        GraphQL.newGraphQL(schema)
            .instrumentation(new DeprecatedFieldUsageInstrumentation(meterRegistry))
            .build();
  }

  @Test
  public void testCountsEachDeprecatedFieldOncePerRequest() {
    execute("{ current legacy items { name oldName } }");
    execute("query named { legacy }");

    assertEquals(count("Query.legacy"), 2.0);
    // oldName is fetched once per list item but counted once per request.
    assertEquals(count("Item.oldName"), 1.0);
    assertNull(
        meterRegistry
            .find(DeprecatedFieldUsageInstrumentation.METRIC_NAME)
            .tag(DeprecatedFieldUsageInstrumentation.FIELD_TAG, "Query.current")
            .counter());
  }

  @Test
  public void testNonDeprecatedFieldsAreNotCounted() {
    execute("{ current items { name } }");

    assertTrue(
        meterRegistry.find(DeprecatedFieldUsageInstrumentation.METRIC_NAME).counters().isEmpty());
  }

  @Test
  public void testRecordsUsageWithoutQueryContext() {
    final ExecutionResult result = graphQL.execute(ExecutionInput.newExecutionInput("{ legacy }"));

    assertTrue(result.getErrors().isEmpty());
    assertEquals(count("Query.legacy"), 1.0);
  }

  private void execute(final String query) {
    final QueryContext context = mock(QueryContext.class);
    when(context.getActorUrn()).thenReturn("urn:li:corpuser:test");
    final ExecutionResult result =
        graphQL.execute(
            ExecutionInput.newExecutionInput(query)
                .graphQLContext(Map.of(QueryContext.class, context))
                .build());
    assertTrue(result.getErrors().isEmpty(), result.getErrors().toString());
  }

  private double count(final String coordinate) {
    final Counter counter =
        meterRegistry
            .find(DeprecatedFieldUsageInstrumentation.METRIC_NAME)
            .tag(DeprecatedFieldUsageInstrumentation.FIELD_TAG, coordinate)
            .counter();
    return counter == null ? 0.0 : counter.count();
  }
}
