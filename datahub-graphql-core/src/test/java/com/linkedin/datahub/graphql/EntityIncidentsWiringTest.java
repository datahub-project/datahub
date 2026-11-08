package com.linkedin.datahub.graphql;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertTrue;

import com.linkedin.datahub.graphql.resolvers.incident.EntityIncidentsResolver;
import com.linkedin.entity.client.EntityClient;
import graphql.schema.FieldCoordinates;
import graphql.schema.GraphQLFieldDefinition;
import graphql.schema.GraphQLNamedType;
import graphql.schema.GraphQLObjectType;
import graphql.schema.GraphQLSchema;
import graphql.schema.GraphQLTypeUtil;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.ArrayList;
import java.util.List;
import org.testng.annotations.Test;

/**
 * An {@code incidents: EntityIncidentsResult} field without {@link EntityIncidentsResolver} is not
 * a schema error: it silently resolves to null. This walks the built schema, so it covers fields
 * declared in any schema file, not only the ones the wiring reads.
 */
public class EntityIncidentsWiringTest {

  @Test
  public void everyEntityIncidentsFieldIsWired() throws Exception {
    GraphQLSchema schema =
        MockedGmsGraphQLEngine.build(
                TestOperationContexts.defaultEntityRegistry(), mock(EntityClient.class))
            .getGraphQL()
            .getGraphQLSchema();

    List<String> wired = new ArrayList<>();
    List<String> problems = new ArrayList<>();
    for (GraphQLNamedType type : schema.getAllTypesAsList()) {
      if (!(type instanceof GraphQLObjectType objectType)) {
        continue;
      }
      GraphQLFieldDefinition field = objectType.getFieldDefinition("incidents");
      if (field == null
          || !(GraphQLTypeUtil.unwrapNonNull(field.getType()) instanceof GraphQLObjectType result)
          || !result.getName().equals("EntityIncidentsResult")) {
        continue;
      }
      if (!(schema
              .getCodeRegistry()
              .getDataFetcher(FieldCoordinates.coordinates(objectType, field), field)
          instanceof EntityIncidentsResolver)) {
        problems.add(
            objectType.getName()
                + " is not wired; add `extend type "
                + objectType.getName()
                + " implements HasIncidents` to interfaces/incidents.graphql");
      } else if (objectType.getInterfaces().stream()
          .noneMatch(iface -> iface.getName().equals("Entity"))) {
        problems.add(objectType.getName() + " does not implement Entity");
      } else {
        wired.add(objectType.getName());
      }
    }

    assertTrue(problems.isEmpty(), String.join("\n", problems));
    assertTrue(wired.contains("Dataset"), wired.toString());
  }
}
