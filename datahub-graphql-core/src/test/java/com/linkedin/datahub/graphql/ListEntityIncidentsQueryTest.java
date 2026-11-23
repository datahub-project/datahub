package com.linkedin.datahub.graphql;

import static com.linkedin.datahub.graphql.Constants.HAS_INCIDENTS_INTERFACE;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.entity.client.EntityClient;
import graphql.ExecutionInput;
import graphql.ParseAndValidate;
import graphql.schema.GraphQLSchema;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.testng.annotations.Test;

/**
 * The MCP {@code list_incidents} tool selects {@code incidents} with one inline fragment per type,
 * so a type that gains {@code HasIncidents} stays invisible to it until the query file gets a
 * fragment for it.
 */
public class ListEntityIncidentsQueryTest {

  private static final Path QUERY_FILE =
      Path.of(
          "../datahub-agent-context/src/datahub_agent_context/mcp_tools/gql/list_entity_incidents.gql");

  /**
   * Types whose {@code incidents} field older GMS versions already serve. Do not add to this list:
   * a new type must be tagged {@code #[NEWER_GMS]} so the MCP tool can drop it on older GMS.
   */
  private static final List<String> UNTAGGED_TYPES =
      List.of("Dataset", "DataJob", "DataFlow", "Dashboard", "Chart");

  private static final Pattern FRAGMENT_TYPE = Pattern.compile("\\.\\.\\. on (\\w+) \\{");

  @Test
  public void queryHasOneFragmentPerHasIncidentsType() throws IOException {
    List<String> expectedTypes =
        orderTypes(GmsGraphQLEngine.typesImplementing(HAS_INCIDENTS_INTERFACE));
    String expectedFragments =
        expectedTypes.stream()
            .map(ListEntityIncidentsQueryTest::renderFragment)
            .collect(Collectors.joining());
    String query = readQuery();

    List<String> actualTypes = new ArrayList<>();
    Matcher matcher = FRAGMENT_TYPE.matcher(query);
    while (matcher.find()) {
      actualTypes.add(matcher.group(1));
    }

    assertEquals(actualTypes, expectedTypes, QUERY_FILE + " has the wrong type fragments");
    assertTrue(
        query.contains(expectedFragments),
        QUERY_FILE + " must contain these type fragments verbatim:\n" + expectedFragments);
  }

  @Test
  public void queryValidatesAgainstSchema() throws Exception {
    GraphQLSchema schema =
        MockedGmsGraphQLEngine.build(
                TestOperationContexts.defaultEntityRegistry(), mock(EntityClient.class))
            .getGraphQL()
            .getGraphQLSchema();

    assertEquals(
        ParseAndValidate.parseAndValidate(
                schema, ExecutionInput.newExecutionInput(readQuery()).build())
            .getErrors(),
        List.of());
  }

  /** Normalizes line endings so a CRLF checkout compares equal to the rendered fragments. */
  private static String readQuery() throws IOException {
    return Files.readString(QUERY_FILE).replace("\r\n", "\n");
  }

  private static List<String> orderTypes(List<String> types) {
    List<String> ordered = new ArrayList<>(UNTAGGED_TYPES);
    ordered.retainAll(types);
    types.stream().filter(type -> !UNTAGGED_TYPES.contains(type)).sorted().forEach(ordered::add);
    return ordered;
  }

  private static String renderFragment(String type) {
    String fragment =
        "        ... on "
            + type
            + " {\n"
            + "            incidents(state: $state, start: $start, count: $count) {\n"
            + "                ...entityIncidentsResultFields\n"
            + "            }\n"
            + "        }\n";
    if (UNTAGGED_TYPES.contains(type)) {
      return fragment;
    }
    return fragment.lines().map(line -> line + " #[NEWER_GMS]\n").collect(Collectors.joining());
  }
}
