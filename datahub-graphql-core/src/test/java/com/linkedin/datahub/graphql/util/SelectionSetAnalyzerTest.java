package com.linkedin.datahub.graphql.util;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import graphql.language.Document;
import graphql.language.Field;
import graphql.language.FragmentDefinition;
import graphql.language.OperationDefinition;
import graphql.parser.Parser;
import graphql.schema.DataFetchingEnvironment;
import graphql.schema.GraphQLSchema;
import graphql.schema.idl.RuntimeWiring;
import graphql.schema.idl.SchemaGenerator;
import graphql.schema.idl.SchemaParser;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.testng.annotations.Test;

public class SelectionSetAnalyzerTest {

  private static final String SDL =
      "type Query { entity: Entity }\n"
          + "interface Entity { urn: String }\n"
          + "type Dataset implements Entity { urn: String, ownership: String, subTypes: String,"
          + " properties: DatasetProperties }\n"
          + "type DatasetProperties { name: String, description: String }\n"
          + "type CorpUser implements Entity { urn: String, slackUserInfo: String }\n";

  private static final GraphQLSchema SCHEMA =
      new SchemaGenerator()
          .makeExecutableSchema(
              new SchemaParser().parse(SDL),
              RuntimeWiring.newRuntimeWiring()
                  .type("Entity", builder -> builder.typeResolver(env -> null))
                  .build());

  /**
   * Parses {@code query} and returns an environment positioned at the top-level {@code entity}
   * field. The selection set mock is strict: touching it fails the test, which is what guards
   * against reintroducing the expensive full-subtree traversal.
   */
  private static DataFetchingEnvironment envFor(String query) {
    Document document = Parser.parse(query);
    Field entityField = null;
    Map<String, FragmentDefinition> fragments = new HashMap<>();
    for (var definition : document.getDefinitions()) {
      if (definition instanceof OperationDefinition) {
        entityField =
            (Field) ((OperationDefinition) definition).getSelectionSet().getSelections().get(0);
      } else if (definition instanceof FragmentDefinition) {
        FragmentDefinition fragment = (FragmentDefinition) definition;
        fragments.put(fragment.getName(), fragment);
      }
    }
    DataFetchingEnvironment environment = mock(DataFetchingEnvironment.class);
    when(environment.getField()).thenReturn(entityField);
    when(environment.getFragmentsByName()).thenReturn(fragments);
    when(environment.getGraphQLSchema()).thenReturn(SCHEMA);
    return environment;
  }

  @Test
  public void testCollectsOnlyImmediateFields() {
    DataFetchingEnvironment environment =
        envFor("{ entity { ownership properties { name description } } }");

    Set<String> fields = SelectionSetAnalyzer.collectImmediateFieldNames(environment, "Dataset");

    assertEquals(fields, Set.of("ownership", "properties"));
    assertFalse(fields.contains("name"), "nested fields belong to DatasetProperties, not Dataset");
  }

  @Test
  public void testInlineFragmentsFilteredByConcreteType() {
    DataFetchingEnvironment environment =
        envFor("{ entity { urn ... on Dataset { ownership } ... on CorpUser { slackUserInfo } } }");

    assertEquals(
        SelectionSetAnalyzer.collectImmediateFieldNames(environment, "Dataset"),
        Set.of("urn", "ownership"));
    assertEquals(
        SelectionSetAnalyzer.collectImmediateFieldNames(environment, "CorpUser"),
        Set.of("urn", "slackUserInfo"));
  }

  @Test
  public void testInterfaceTypeConditionAppliesToImplementors() {
    DataFetchingEnvironment environment = envFor("{ entity { ... on Entity { urn } } }");

    assertTrue(
        SelectionSetAnalyzer.collectImmediateFieldNames(environment, "Dataset").contains("urn"));
  }

  @Test
  public void testNamedFragmentSpreadsResolvedAndFiltered() {
    DataFetchingEnvironment environment =
        envFor(
            "{ entity { ...datasetFields ...userFields } }\n"
                + "fragment datasetFields on Dataset { ownership subTypes }\n"
                + "fragment userFields on CorpUser { slackUserInfo }\n");

    assertEquals(
        SelectionSetAnalyzer.collectImmediateFieldNames(environment, "Dataset"),
        Set.of("ownership", "subTypes"));
  }

  /**
   * The full selection set is what previously exhausted the GMS heap: it materializes every field
   * at every depth on each resolved entity.
   */
  @Test
  public void testDoesNotTouchDataFetchingFieldSelectionSet() {
    DataFetchingEnvironment environment =
        envFor("{ entity { ownership properties { name description } } }");

    SelectionSetAnalyzer.collectImmediateFieldNames(environment, "Dataset");

    verify(environment, never()).getSelectionSet();
  }
}
