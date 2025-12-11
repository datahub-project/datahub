package com.linkedin.datahub.graphql;

import static org.testng.Assert.*;

import graphql.Scalars;
import graphql.schema.*;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class AspectMappingRegistryTest {

  private GraphQLSchema schema;
  private AspectMappingRegistry registry;

  @BeforeMethod
  public void setup() {
    // Create directives
    GraphQLDirective aspectMappingDirective =
        GraphQLDirective.newDirective()
            .name("aspectMapping")
            .argument(
                GraphQLArgument.newArgument()
                    .name("aspects")
                    .type(GraphQLList.list(GraphQLNonNull.nonNull(Scalars.GraphQLString)))
                    .build())
            .validLocation(graphql.introspection.Introspection.DirectiveLocation.FIELD_DEFINITION)
            .build();

    GraphQLDirective noAspectsDirective =
        GraphQLDirective.newDirective()
            .name("noAspects")
            .validLocation(graphql.introspection.Introspection.DirectiveLocation.FIELD_DEFINITION)
            .build();

    // Create a test GraphQL schema with annotated fields
    GraphQLObjectType datasetType =
        GraphQLObjectType.newObject()
            .name("Dataset")
            .field(
                GraphQLFieldDefinition.newFieldDefinition()
                    .name("urn")
                    .type(Scalars.GraphQLString)
                    .withDirective(noAspectsDirective)
                    .build())
            .field(
                GraphQLFieldDefinition.newFieldDefinition()
                    .name("name")
                    .type(Scalars.GraphQLString)
                    .withDirective(
                        aspectMappingDirective,
                        directive ->
                            directive.argument(
                                arg ->
                                    arg.name("aspects")
                                        .valueLiteral(
                                            graphql.language.ArrayValue.newArrayValue()
                                                .value(
                                                    graphql.language.StringValue.newStringValue(
                                                            "datasetProperties")
                                                        .build())
                                                .value(
                                                    graphql.language.StringValue.newStringValue(
                                                            "datasetKey")
                                                        .build())
                                                .build())))
                    .build())
            .field(
                GraphQLFieldDefinition.newFieldDefinition()
                    .name("ownership")
                    .type(Scalars.GraphQLString)
                    .withDirective(
                        aspectMappingDirective,
                        directive ->
                            directive.argument(
                                arg ->
                                    arg.name("aspects")
                                        .valueLiteral(
                                            graphql.language.ArrayValue.newArrayValue()
                                                .value(
                                                    graphql.language.StringValue.newStringValue(
                                                            "ownership")
                                                        .build())
                                                .build())))
                    .build())
            .field(
                GraphQLFieldDefinition.newFieldDefinition()
                    .name("unmappedField")
                    .type(Scalars.GraphQLString)
                    .build())
            .field(
                GraphQLFieldDefinition.newFieldDefinition()
                    .name("__typename")
                    .type(Scalars.GraphQLString)
                    .build())
            .build();

    GraphQLObjectType queryType =
        GraphQLObjectType.newObject()
            .name("Query")
            .field(
                GraphQLFieldDefinition.newFieldDefinition()
                    .name("dataset")
                    .type(datasetType)
                    .build())
            .build();

    schema =
        GraphQLSchema.newSchema()
            .query(queryType)
            .additionalDirective(aspectMappingDirective)
            .additionalDirective(noAspectsDirective)
            .build();

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
    return new graphql.schema.SelectedField() {
      @Override
      public String getName() {
        return fieldName;
      }

      @Override
      public String getQualifiedName() {
        return typeName + "." + fieldName;
      }

      @Override
      public GraphQLFieldDefinition getFieldDefinition() {
        return null;
      }

      @Override
      public java.util.Map<String, Object> getArguments() {
        return Collections.emptyMap();
      }

      @Override
      public graphql.schema.GraphQLOutputType getType() {
        return null;
      }

      @Override
      public List<graphql.schema.SelectedField> getSelectionSet() {
        return Collections.emptyList();
      }

      @Override
      public int getLevel() {
        return 0;
      }

      @Override
      public boolean isConditional() {
        return false;
      }

      @Override
      public String getFullyQualifiedName() {
        return typeName + "." + fieldName;
      }

      @Override
      public String getResultKey() {
        return fieldName;
      }

      @Override
      public GraphQLObjectType getObjectType() {
        return null;
      }

      @Override
      public List<String> getObjectTypeNames() {
        return Arrays.asList(typeName);
      }

      @Override
      public graphql.schema.GraphQLFieldsContainer getFieldsContainer() {
        return null;
      }
    };
  }
}
