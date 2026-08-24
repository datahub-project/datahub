package com.linkedin.datahub.graphql.resolvers.incident;

import graphql.language.ObjectTypeDefinition;
import graphql.language.ObjectTypeExtensionDefinition;
import graphql.language.TypeDefinition;
import graphql.schema.idl.SchemaParser;
import graphql.schema.idl.TypeDefinitionRegistry;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Reads which GraphQL types expose {@code incidents} from incident.graphql so {@code
 * GmsGraphQLEngine} does not keep a parallel hardcoded list. SDL without a matching dataFetcher
 * resolves through PropertyDataFetcher and returns silent null.
 */
public final class IncidentSdl {

  private static final String INCIDENTS_FIELD = "incidents";
  private static final Set<String> ROOT_OPERATION_TYPES =
      Set.of("Query", "Mutation", "Subscription");

  private IncidentSdl() {}

  public static List<String> typesDeclaringIncidentsField(String sdl) {
    TypeDefinitionRegistry registry = new SchemaParser().parse(sdl);
    LinkedHashSet<String> types = new LinkedHashSet<>();
    collect(types, registry.types().values());
    for (List<ObjectTypeExtensionDefinition> extensions :
        registry.objectTypeExtensions().values()) {
      collect(types, extensions);
    }
    types.removeAll(ROOT_OPERATION_TYPES);
    return List.copyOf(types);
  }

  private static void collect(
      Set<String> types, Collection<? extends TypeDefinition<?>> definitions) {
    for (TypeDefinition<?> definition : definitions) {
      if (!(definition instanceof ObjectTypeDefinition)) {
        continue;
      }
      ObjectTypeDefinition objectType = (ObjectTypeDefinition) definition;
      boolean hasIncidents =
          objectType.getFieldDefinitions().stream()
              .anyMatch(field -> INCIDENTS_FIELD.equals(field.getName()));
      if (hasIncidents) {
        types.add(objectType.getName());
      }
    }
  }
}
