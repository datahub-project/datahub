package com.linkedin.datahub.graphql;

import static org.testng.Assert.*;

import com.linkedin.data.schema.annotation.PathSpecBasedSchemaAnnotationVisitor;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.ConfigEntityRegistry;
import graphql.language.ArrayValue;
import graphql.language.FieldDefinition;
import graphql.language.InterfaceTypeDefinition;
import graphql.language.ObjectTypeDefinition;
import graphql.language.StringValue;
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
import java.util.Map;
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
  private Set<String> entityTypes;
  private ConfigEntityRegistry modelRegistry;

  @BeforeClass
  public void loadSchemas() {
    PathSpecBasedSchemaAnnotationVisitor.class
        .getClassLoader()
        .setClassAssertionStatus(PathSpecBasedSchemaAnnotationVisitor.class.getName(), false);
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
    entityTypes = discoverEntityObjectTypes(typeRegistry);
    assertFalse(entityTypes.isEmpty(), "Expected to discover Entity-implementing object types");
    modelRegistry =
        new ConfigEntityRegistry(
            Objects.requireNonNull(
                cl.getResourceAsStream("entity-registry.yml"), "Missing entity-registry.yml"));
  }

  @Test
  public void testAllEntityFieldsHaveAspectDirectives() {
    List<String> missing = new ArrayList<>();
    Set<String> seenTypes = new HashSet<>();

    for (String typeName : entityTypes) {
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
            boolean hasFetchAllAspects =
                field.getDirectives().stream().anyMatch(d -> d.getName().equals("fetchAllAspects"));
            if (hasAspectMapping || hasNoAspects || hasFetchAllAspects) {
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
    assertEquals(seenTypes, entityTypes, "Expected to resolve all discovered entity types");
  }

  @Test
  public void testEntityTypesImplementEntityInterface() {
    List<String> notEntity = new ArrayList<>();
    for (String typeName : entityTypes) {
      List<ObjectTypeDefinition> defs = collectObjectTypeDefinitions(typeName);
      if (defs.isEmpty()) {
        continue;
      }
      boolean anyImplementsEntity =
          defs.stream()
              .flatMap(d -> d.getImplements().stream())
              .anyMatch(t -> t instanceof TypeName && ((TypeName) t).getName().equals("Entity"));
      if (!anyImplementsEntity) {
        notEntity.add(typeName);
      }
    }
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

  /**
   * Aspect directives belong on concrete object types. {@link AspectMappingRegistry} skips
   * interfaces; annotations on interface fields would be ignored and are therefore forbidden.
   */
  @Test
  public void testInterfaceFieldsHaveNoAspectDirectives() {
    List<String> annotated = new ArrayList<>();
    for (Map.Entry<String, TypeDefinition> entry : typeRegistry.types().entrySet()) {
      TypeDefinition<?> def = entry.getValue();
      if (!(def instanceof InterfaceTypeDefinition)) {
        continue;
      }
      collectAnnotatedInterfaceFields((InterfaceTypeDefinition) def, annotated);
    }
    for (Map.Entry<String, List<graphql.language.InterfaceTypeExtensionDefinition>> entry :
        typeRegistry.interfaceTypeExtensions().entrySet()) {
      for (graphql.language.InterfaceTypeExtensionDefinition ext : entry.getValue()) {
        collectAnnotatedInterfaceFields(ext, annotated);
      }
    }
    assertTrue(
        annotated.isEmpty(),
        "Interface fields must not declare @aspectMapping/@noAspects (place them on concrete"
            + " object types):\n"
            + annotated.stream().sorted().collect(Collectors.joining("\n")));
  }

  @Test
  public void testMappedAspectNamesExistInEntityRegistry() {
    Set<String> validAspectNames = modelRegistry.getAspectSpecs().keySet();
    List<String> invalid = new ArrayList<>();

    typeRegistry.types().values().stream()
        .filter(ObjectTypeDefinition.class::isInstance)
        .map(ObjectTypeDefinition.class::cast)
        .forEach(definition -> collectInvalidAspectNames(definition, validAspectNames, invalid));
    typeRegistry.objectTypeExtensions().values().stream()
        .flatMap(List::stream)
        .forEach(definition -> collectInvalidAspectNames(definition, validAspectNames, invalid));

    assertTrue(
        invalid.isEmpty(),
        "@aspectMapping references aspect names absent from entity-registry.yml:\n"
            + invalid.stream().sorted().collect(Collectors.joining("\n")));
  }

  @Test
  public void testEachEntityMappingMatchesAnEntityRegistrySpec() {
    List<String> unmatched = new ArrayList<>();
    for (String typeName : entityTypes) {
      Set<String> mappedAspects =
          collectObjectTypeDefinitions(typeName).stream()
              .flatMap(definition -> definition.getFieldDefinitions().stream())
              .flatMap(field -> mappedAspectNames(field).stream())
              .collect(Collectors.toSet());
      if (mappedAspects.isEmpty()) {
        continue;
      }

      boolean hasMatchingEntitySpec =
          modelRegistry.getEntitySpecs().values().stream()
              .map(EntitySpec::getAspectSpecs)
              .map(specs -> specs.stream().map(spec -> spec.getName()).collect(Collectors.toSet()))
              .anyMatch(aspectNames -> aspectNames.containsAll(mappedAspects));
      if (!hasMatchingEntitySpec) {
        unmatched.add(typeName + ": " + mappedAspects);
      }
    }

    assertTrue(
        unmatched.isEmpty(),
        "No entity-registry.yml entity contains all aspects mapped for these GraphQL types:\n"
            + unmatched.stream().sorted().collect(Collectors.joining("\n")));
  }

  private static void collectInvalidAspectNames(
      ObjectTypeDefinition type, Set<String> validAspectNames, List<String> invalid) {
    for (FieldDefinition field : type.getFieldDefinitions()) {
      mappedAspectNames(field).stream()
          .filter(aspectName -> !validAspectNames.contains(aspectName))
          .forEach(
              aspectName ->
                  invalid.add(type.getName() + "." + field.getName() + ": " + aspectName));
    }
  }

  private static Set<String> mappedAspectNames(FieldDefinition field) {
    return field.getDirectives("aspectMapping").stream()
        .flatMap(directive -> directive.getArguments().stream())
        .filter(argument -> argument.getName().equals("aspects"))
        .map(argument -> argument.getValue())
        .filter(ArrayValue.class::isInstance)
        .map(ArrayValue.class::cast)
        .flatMap(value -> value.getValues().stream())
        .filter(StringValue.class::isInstance)
        .map(StringValue.class::cast)
        .map(StringValue::getValue)
        .collect(Collectors.toSet());
  }

  private static void collectAnnotatedInterfaceFields(
      InterfaceTypeDefinition iface, List<String> annotated) {
    for (FieldDefinition field : iface.getFieldDefinitions()) {
      boolean hasAspectMapping =
          field.getDirectives().stream().anyMatch(d -> d.getName().equals("aspectMapping"));
      boolean hasNoAspects =
          field.getDirectives().stream().anyMatch(d -> d.getName().equals("noAspects"));
      boolean hasFetchAllAspects =
          field.getDirectives().stream().anyMatch(d -> d.getName().equals("fetchAllAspects"));
      if (hasAspectMapping || hasNoAspects || hasFetchAllAspects) {
        annotated.add(iface.getName() + "." + field.getName());
      }
    }
  }

  /**
   * Object types that implement the {@code Entity} interface (on the primary definition). Derived
   * from the schema so newly added entities are covered automatically.
   */
  private static Set<String> discoverEntityObjectTypes(TypeDefinitionRegistry registry) {
    Set<String> types = new HashSet<>();
    for (Map.Entry<String, TypeDefinition> entry : registry.types().entrySet()) {
      TypeDefinition<?> def = entry.getValue();
      if (!(def instanceof ObjectTypeDefinition)) {
        continue;
      }
      ObjectTypeDefinition objectType = (ObjectTypeDefinition) def;
      boolean implementsEntity =
          objectType.getImplements().stream()
              .anyMatch(t -> t instanceof TypeName && ((TypeName) t).getName().equals("Entity"));
      if (implementsEntity) {
        types.add(objectType.getName());
      }
    }
    return Set.copyOf(types);
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
