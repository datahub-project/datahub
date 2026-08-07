package com.linkedin.datahub.graphql;

import static org.testng.Assert.*;

import graphql.language.FieldDefinition;
import graphql.language.ObjectTypeDefinition;
import graphql.language.TypeDefinition;
import graphql.language.TypeName;
import graphql.schema.idl.SchemaParser;
import graphql.schema.idl.TypeDefinitionRegistry;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Ensures every field on GraphQL Entity object types (and extensions) has either {@code
 * @aspectMapping} or {@code @noAspects}. Without complete coverage, aspect optimization falls back
 * to fetching all aspects whenever an unmapped field is selected.
 */
public class AspectMappingCompletenessTest {

  private static final Set<String> ENTITY_TYPES =
      Set.of(
          "AccessTokenMetadata",
          "Application",
          "Assertion",
          "BusinessAttribute",
          "Chart",
          "Container",
          "CorpGroup",
          "CorpUser",
          "Dashboard",
          "DataContract",
          "DataFlow",
          "DataHubConnection",
          "DataHubFile",
          "DataHubPageModule",
          "DataHubPageTemplate",
          "DataHubPolicy",
          "DataHubRole",
          "DataHubView",
          "DataJob",
          "DataPlatform",
          "DataPlatformInstance",
          "DataProcessInstance",
          "DataProduct",
          "DataTypeEntity",
          "Dataset",
          "Document",
          "Domain",
          "ERModelRelationship",
          "EntityTypeEntity",
          "ExecutionRequest",
          "Form",
          "GlossaryNode",
          "GlossaryTerm",
          "Incident",
          "Metric",
          "MLFeature",
          "MLFeatureTable",
          "MLModel",
          "MLModelGroup",
          "MLPrimaryKey",
          "Notebook",
          "OwnershipTypeEntity",
          "Post",
          "QueryEntity",
          "Restricted",
          "Role",
          "SchemaFieldEntity",
          "SemanticModel",
          "ServiceAccount",
          "StructuredPropertyEntity",
          "Tag",
          "Test",
          "VersionSet",
          "VersionedDataset");

  private static final String[] SCHEMA_FILES = {
    "entity.graphql",
    "app.graphql",
    "auth.graphql",
    "connection.graphql",
    "contract.graphql",
    "documents.graphql",
    "files.graphql",
    "forms.graphql",
    "incident.graphql",
    "ingestion.graphql",
    "logical.graphql",
    "metrics.graphql",
    "module.graphql",
    "properties.graphql",
    "runs.graphql",
    "settings.graphql",
    "template.graphql",
    "tests.graphql",
    "timeseries.graphql",
    "versioning.graphql",
    "assertions.graphql",
    "dataProduct.graphql"
  };

  private TypeDefinitionRegistry typeRegistry;

  @BeforeClass
  public void loadSchemas() {
    SchemaParser parser = new SchemaParser();
    typeRegistry = new TypeDefinitionRegistry();
    ClassLoader cl = Thread.currentThread().getContextClassLoader();
    for (String file : SCHEMA_FILES) {
      try (Reader reader =
          new InputStreamReader(
              Objects.requireNonNull(cl.getResourceAsStream(file), "Missing schema: " + file),
              StandardCharsets.UTF_8)) {
        typeRegistry.merge(parser.parse(reader));
      } catch (Exception e) {
        throw new RuntimeException("Failed to parse " + file, e);
      }
    }
  }

  @Test
  public void testAllEntityFieldsHaveAspectDirectives() {
    List<String> missing = new ArrayList<>();
    Set<String> seenTypes = new HashSet<>();

    for (String typeName : ENTITY_TYPES) {
      List<ObjectTypeDefinition> defs = collectObjectTypeDefinitions(typeName);
      if (defs.isEmpty()) {
        missing.add(typeName + ".<type missing from schema>");
        continue;
      }
      seenTypes.add(typeName);
      Set<String> fieldNames = new HashSet<>();
      for (ObjectTypeDefinition def : defs) {
        for (FieldDefinition field : def.getFieldDefinitions()) {
          // Extensions can redeclare fields; require at least one annotated definition.
          fieldNames.add(field.getName());
        }
      }
      for (String fieldName : fieldNames) {
        boolean annotated = false;
        for (ObjectTypeDefinition def : defs) {
          for (FieldDefinition field : def.getFieldDefinitions()) {
            if (!field.getName().equals(fieldName)) {
              continue;
            }
            boolean hasAspectMapping =
                field.getDirectives().stream().anyMatch(d -> d.getName().equals("aspectMapping"));
            boolean hasNoAspects =
                field.getDirectives().stream().anyMatch(d -> d.getName().equals("noAspects"));
            if (hasAspectMapping || hasNoAspects) {
              annotated = true;
              break;
            }
          }
          if (annotated) {
            break;
          }
        }
        if (!annotated) {
          missing.add(typeName + "." + fieldName);
        }
      }
    }

    assertTrue(
        missing.isEmpty(),
        "Fields missing @aspectMapping/@noAspects (optimization will fall back to all aspects):\n"
            + missing.stream().sorted().collect(Collectors.joining("\n")));
    assertEquals(seenTypes.size(), ENTITY_TYPES.size(), "Expected to resolve all entity types");
  }

  @Test
  public void testEntityTypesImplementEntityInterface() {
    List<String> notEntity = new ArrayList<>();
    for (String typeName : ENTITY_TYPES) {
      List<ObjectTypeDefinition> defs = collectObjectTypeDefinitions(typeName);
      if (defs.isEmpty()) {
        continue;
      }
      boolean implementsEntity =
          defs.stream()
              .flatMap(d -> d.getImplements().stream())
              .anyMatch(t -> t instanceof TypeName && ((TypeName) t).getName().equals("Entity"));
      // Some entity types only declare implements Entity on the primary definition.
      if (!implementsEntity && defs.stream().noneMatch(d -> !d.getImplements().isEmpty())) {
        // VersionedDataset / Restricted etc. should still implement Entity on primary type.
      }
      boolean anyImplementsEntity =
          defs.stream()
              .flatMap(d -> d.getImplements().stream())
              .anyMatch(t -> t instanceof TypeName && ((TypeName) t).getName().equals("Entity"));
      if (!anyImplementsEntity) {
        notEntity.add(typeName);
      }
    }
    // ServiceAccount and a few types may implement Entity only via primary def; filter empty
    // implements on extend-only merges by checking primary.
    List<String> failures =
        notEntity.stream()
            .filter(
                t -> {
                  TypeDefinition<?> def = typeRegistry.getType(t).orElse(null);
                  if (!(def instanceof ObjectTypeDefinition)) {
                    return true;
                  }
                  return ((ObjectTypeDefinition) def)
                      .getImplements().stream()
                          .noneMatch(
                              i ->
                                  i instanceof TypeName
                                      && ((TypeName) i).getName().equals("Entity"));
                })
            .sorted(Comparator.naturalOrder())
            .collect(Collectors.toList());
    assertTrue(
        failures.isEmpty(),
        "Expected types to implement Entity interface: " + String.join(", ", failures));
  }

  private List<ObjectTypeDefinition> collectObjectTypeDefinitions(String typeName) {
    List<ObjectTypeDefinition> defs = new ArrayList<>();
    typeRegistry
        .getType(typeName)
        .ifPresent(
            def -> {
              if (def instanceof ObjectTypeDefinition) {
                defs.add((ObjectTypeDefinition) def);
              }
            });
    // graphql-java TypeDefinitionRegistry stores extensions separately
    List<graphql.language.ObjectTypeExtensionDefinition> extensions =
        typeRegistry.objectTypeExtensions().getOrDefault(typeName, List.of());
    defs.addAll(extensions);
    return defs;
  }
}
