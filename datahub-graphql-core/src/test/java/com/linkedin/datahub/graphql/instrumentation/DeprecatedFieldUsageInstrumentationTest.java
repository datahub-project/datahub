package com.linkedin.datahub.graphql.instrumentation;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.linkedin.datahub.graphql.QueryContext;
import graphql.ExecutionInput;
import graphql.ExecutionResult;
import graphql.GraphQL;
import graphql.schema.GraphQLSchema;
import graphql.schema.idl.RuntimeWiring;
import graphql.schema.idl.SchemaGenerator;
import graphql.schema.idl.SchemaParser;
import java.util.List;
import java.util.Map;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
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

  private final Logger logger =
      (Logger) LoggerFactory.getLogger(DeprecatedFieldUsageInstrumentation.class);
  private ListAppender<ILoggingEvent> appender;
  private GraphQL graphQL;

  @BeforeMethod
  public void setUp() {
    appender = new ListAppender<>();
    appender.start();
    logger.addAppender(appender);

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
            .instrumentation(new DeprecatedFieldUsageInstrumentation())
            .build();
  }

  @AfterMethod
  public void tearDown() {
    logger.detachAppender(appender);
  }

  @Test
  public void testLogsEachDeprecatedFieldOncePerActor() {
    execute("{ current legacy items { name oldName } }", "urn:li:corpuser:a");
    execute("{ legacy }", "urn:li:corpuser:a");
    execute("{ legacy }", "urn:li:corpuser:b");

    assertEquals(
        messages(),
        List.of(
            "Deprecated GraphQL field Query.legacy was called by urn:li:corpuser:a",
            "Deprecated GraphQL field Item.oldName was called by urn:li:corpuser:a",
            "Deprecated GraphQL field Query.legacy was called by urn:li:corpuser:b"));
    assertTrue(appender.list.stream().allMatch(event -> event.getLevel() == Level.WARN));
  }

  @Test
  public void testLogsUnknownActorWithoutFailingTheRequest() {
    final ExecutionResult result = graphQL.execute(ExecutionInput.newExecutionInput("{ legacy }"));

    assertTrue(result.getErrors().isEmpty(), result.getErrors().toString());
    assertEquals(
        messages(), List.of("Deprecated GraphQL field Query.legacy was called by unknown"));
  }

  @Test
  public void testIgnoresFieldsThatAreNotDeprecated() {
    execute("{ current items { name } }", "urn:li:corpuser:a");

    assertTrue(messages().isEmpty());
  }

  private void execute(final String query, final String actor) {
    final QueryContext context = mock(QueryContext.class);
    when(context.getActor()).thenReturn(new Actor(ActorType.USER, actor));
    when(context.getActorUrn()).thenReturn(actor);
    final ExecutionResult result =
        graphQL.execute(
            ExecutionInput.newExecutionInput(query)
                .graphQLContext(Map.of(QueryContext.class, context))
                .build());
    assertTrue(result.getErrors().isEmpty(), result.getErrors().toString());
  }

  private List<String> messages() {
    return appender.list.stream().map(ILoggingEvent::getFormattedMessage).toList();
  }
}
