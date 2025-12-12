package com.linkedin.datahub.graphql;

import static org.testng.Assert.*;

import graphql.schema.*;
import graphql.schema.idl.RuntimeWiring;
import graphql.schema.idl.SchemaGenerator;
import graphql.schema.idl.SchemaParser;
import graphql.schema.idl.TypeDefinitionRegistry;
import java.util.Arrays;
import java.util.Collections;
import java.util.Set;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class AspectMappingRegistryTest {

  private GraphQLSchema schema;
  private AspectMappingRegistry registry;

  @BeforeMethod
  public void setup() {
    // Parse SDL like production does - this is how the real schema is built
    String schemaDefinition =
        """
        directive @aspectMapping(aspects: [String!]!) on FIELD_DEFINITION
        directive @noAspects on FIELD_DEFINITION

        type Query {
          dataset: Dataset
        }

        type Dataset {
          urn: String! @noAspects
          name: String! @aspectMapping(aspects: ["datasetProperties", "datasetKey"])
          ownership: String @aspectMapping(aspects: ["ownership"])
          unmappedField: String
          __typename: String
        }
        """;

    SchemaParser schemaParser = new SchemaParser();
    TypeDefinitionRegistry typeRegistry = schemaParser.parse(schemaDefinition);

    RuntimeWiring runtimeWiring = RuntimeWiring.newRuntimeWiring().build();

    SchemaGenerator schemaGenerator = new SchemaGenerator();
    schema = schemaGenerator.makeExecutableSchema(typeRegistry, runtimeWiring);

    registry = new AspectMappingRegistry(schema);
  }

  @Test
  public void testParsesAspectMappingDirective() {
    graphql.schema.SelectedField nameField = createMockSelectedField("name", "Dataset");

    Set<String> aspects = registry.getRequiredAspects("Dataset", Arrays.asList(nameField));

    assertNotNull(aspects);
    assertEquals(aspects.size(), 2);
    assertTrue(aspects.contains("datasetProperties"));
    assertTrue(aspects.contains("datasetKey"));
  }

  @Test
  public void testParsesNoAspectsDirective() {
    graphql.schema.SelectedField urnField = createMockSelectedField("urn", "Dataset");

    Set<String> aspects = registry.getRequiredAspects("Dataset", Arrays.asList(urnField));

    assertNotNull(aspects);
    assertTrue(aspects.isEmpty());
  }

  @Test
  public void testMultipleFieldsAccumulate() {
    graphql.schema.SelectedField nameField = createMockSelectedField("name", "Dataset");
    graphql.schema.SelectedField ownershipField = createMockSelectedField("ownership", "Dataset");

    Set<String> aspects =
        registry.getRequiredAspects("Dataset", Arrays.asList(nameField, ownershipField));

    assertNotNull(aspects);
    assertEquals(aspects.size(), 3);
    assertTrue(aspects.contains("datasetProperties"));
    assertTrue(aspects.contains("datasetKey"));
    assertTrue(aspects.contains("ownership"));
  }

  @Test
  public void testUnmappedFieldReturnsNull() {
    graphql.schema.SelectedField unmappedField =
        createMockSelectedField("unmappedField", "Dataset");

    Set<String> aspects = registry.getRequiredAspects("Dataset", Arrays.asList(unmappedField));

    assertNull(aspects, "Unmapped field should return null to trigger fallback");
  }

  @Test
  public void testIntrospectionFieldsSkipped() {
    graphql.schema.SelectedField typenameField = createMockSelectedField("__typename", "Dataset");
    graphql.schema.SelectedField nameField = createMockSelectedField("name", "Dataset");

    Set<String> aspects =
        registry.getRequiredAspects("Dataset", Arrays.asList(typenameField, nameField));

    assertNotNull(aspects);
    assertEquals(aspects.size(), 2);
    assertTrue(aspects.contains("datasetProperties"));
    assertTrue(aspects.contains("datasetKey"));
  }

  @Test
  public void testEmptySelectionReturnsEmpty() {
    Set<String> aspects = registry.getRequiredAspects("Dataset", Collections.emptyList());

    assertNotNull(aspects);
    assertTrue(aspects.isEmpty());
  }

  @Test
  public void testOnlyIntrospectionFieldsReturnsEmpty() {
    graphql.schema.SelectedField typenameField = createMockSelectedField("__typename", "Dataset");
    graphql.schema.SelectedField schemaField = createMockSelectedField("__schema", "Dataset");

    Set<String> aspects =
        registry.getRequiredAspects("Dataset", Arrays.asList(typenameField, schemaField));

    assertNotNull(aspects);
    assertTrue(aspects.isEmpty());
  }

  @Test
  public void testMixedMappedAndUnmappedReturnsNull() {
    graphql.schema.SelectedField nameField = createMockSelectedField("name", "Dataset");
    graphql.schema.SelectedField unmappedField =
        createMockSelectedField("unmappedField", "Dataset");

    Set<String> aspects =
        registry.getRequiredAspects("Dataset", Arrays.asList(nameField, unmappedField));

    assertNull(aspects, "Any unmapped field should trigger fallback");
  }

  @Test
  public void testTypeNameFilteringIgnoresOtherTypes() {
    graphql.schema.SelectedField nameFieldForDataset = createMockSelectedField("name", "Dataset");
    graphql.schema.SelectedField nameFieldForSearchResult =
        createMockSelectedField("name", "SearchResult");

    Set<String> aspects =
        registry.getRequiredAspects(
            "Dataset", Arrays.asList(nameFieldForDataset, nameFieldForSearchResult));

    assertNotNull(aspects);
    assertEquals(aspects.size(), 2);
    assertTrue(aspects.contains("datasetProperties"));
    assertTrue(aspects.contains("datasetKey"));
  }

  @Test
  public void testDuplicateAspectsDeduped() {
    graphql.schema.SelectedField nameField1 = createMockSelectedField("name", "Dataset");
    graphql.schema.SelectedField nameField2 = createMockSelectedField("name", "Dataset");

    Set<String> aspects =
        registry.getRequiredAspects("Dataset", Arrays.asList(nameField1, nameField2));

    assertNotNull(aspects);
    assertEquals(aspects.size(), 2);
    assertTrue(aspects.contains("datasetProperties"));
    assertTrue(aspects.contains("datasetKey"));
  }

  private graphql.schema.SelectedField createMockSelectedField(String fieldName, String typeName) {
    graphql.schema.SelectedField field = Mockito.mock(graphql.schema.SelectedField.class);
    Mockito.when(field.getName()).thenReturn(fieldName);
    Mockito.when(field.getQualifiedName()).thenReturn(typeName + "." + fieldName);
    Mockito.when(field.getFullyQualifiedName()).thenReturn(typeName + "." + fieldName);
    Mockito.when(field.getResultKey()).thenReturn(fieldName);
    Mockito.when(field.getObjectTypeNames()).thenReturn(Arrays.asList(typeName));
    return field;
  }
}
