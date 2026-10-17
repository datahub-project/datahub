package com.linkedin.datahub.graphql.codegen;

import graphql.language.FieldDefinition;
import graphql.language.NonNullType;
import graphql.language.ObjectTypeDefinition;
import graphql.language.ObjectTypeExtensionDefinition;
import graphql.language.Type;
import graphql.language.TypeDefinition;
import graphql.language.TypeName;
import graphql.schema.idl.SchemaParser;
import graphql.schema.idl.TypeDefinitionRegistry;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Finds GraphQL types that declare {@code incidents: EntityIncidentsResult}. Matching on field name
 * alone is wrong: {@code EntityIncidentsResult} itself has {@code incidents: [Incident!]!}.
 */
public final class IncidentSdl {

  private static final String INCIDENTS_FIELD = "incidents";
  private static final String ENTITY_INCIDENTS_RESULT_TYPE = "EntityIncidentsResult";
  private static final Set<String> ROOT_OPERATION_TYPES =
      Set.of("Query", "Mutation", "Subscription");

  private IncidentSdl() {}

  public static List<String> typesDeclaringIncidentsField(String sdl) {
    TypeDefinitionRegistry registry = new SchemaParser().parse(sdl);
    LinkedHashSet<String> types = new LinkedHashSet<>();
    for (TypeDefinition<?> definition : registry.types().values()) {
      addIfEntityIncidentsType(types, definition);
    }
    for (List<ObjectTypeExtensionDefinition> extensions :
        registry.objectTypeExtensions().values()) {
      for (ObjectTypeExtensionDefinition extension : extensions) {
        addIfEntityIncidentsType(types, extension);
      }
    }
    types.removeAll(ROOT_OPERATION_TYPES);
    types.remove(ENTITY_INCIDENTS_RESULT_TYPE);
    return List.copyOf(types);
  }

  private static void addIfEntityIncidentsType(Set<String> types, TypeDefinition<?> definition) {
    if (!(definition instanceof ObjectTypeDefinition objectType)) {
      return;
    }
    boolean hasEntityIncidentsField =
        objectType.getFieldDefinitions().stream().anyMatch(IncidentSdl::isEntityIncidentsField);
    if (hasEntityIncidentsField) {
      types.add(objectType.getName());
    }
  }

  private static boolean isEntityIncidentsField(FieldDefinition field) {
    return INCIDENTS_FIELD.equals(field.getName())
        && ENTITY_INCIDENTS_RESULT_TYPE.equals(namedTypeIfNotList(field.getType()));
  }

  /** {@code EntityIncidentsResult} or {@code EntityIncidentsResult!}, not a list. */
  private static String namedTypeIfNotList(Type type) {
    Type current = type;
    while (current instanceof NonNullType nonNullType) {
      current = nonNullType.getType();
    }
    if (current instanceof TypeName typeName) {
      return typeName.getName();
    }
    return null;
  }
}
