package com.linkedin.datahub.graphql;

import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;

import com.datahub.authentication.AuthenticationConfiguration;
import com.datahub.authorization.AuthorizationConfiguration;
import com.linkedin.datahub.graphql.featureflags.FeatureFlags;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.config.GraphQLConfiguration;
import com.linkedin.metadata.config.IngestionConfiguration;
import com.linkedin.metadata.config.graphql.GraphQLMetricsConfiguration;
import com.linkedin.metadata.config.graphql.GraphQLQueryConfiguration;
import com.linkedin.metadata.models.registry.EntityRegistry;
import graphql.ExecutionResult;
import graphql.schema.GraphQLSchema;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.lang.reflect.Modifier;
import java.util.Map;
import java.util.function.Consumer;

/**
 * Runs a query through the real GraphQL schema and wiring, with the given entity client as the
 * backend and every other service a mock that answers nothing.
 */
final class MockedGmsGraphQLEngine {

  private MockedGmsGraphQLEngine() {}

  static ExecutionResult execute(String query, QueryContext context, EntityClient entityClient)
      throws IllegalAccessException {
    EntityRegistry registry = context.getOperationContext().getEntityRegistry();
    return build(registry, entityClient).execute(query, null, Map.of(), context);
  }

  static GraphQLEngine build(EntityRegistry registry, EntityClient entityClient)
      throws IllegalAccessException {
    return new GmsGraphQLEngine(args(registry, entityClient)).builder().build();
  }

  /** The real schema and wiring, with every service a mock. */
  static GraphQLSchema schema(EntityRegistry registry) throws IllegalAccessException {
    return schema(registry, args -> {});
  }

  /** The real schema and wiring, with every service a mock unless {@code overrides} sets it. */
  static GraphQLSchema schema(EntityRegistry registry, Consumer<GmsGraphQLEngineArgs> overrides)
      throws IllegalAccessException {
    GmsGraphQLEngineArgs args = args(registry, mock(EntityClient.class));
    overrides.accept(args);
    return new GmsGraphQLEngine(args).builder().build().getGraphQL().getGraphQLSchema();
  }

  /**
   * Engine args as production builds them, with the given entity client and other services mocked.
   */
  private static GmsGraphQLEngineArgs args(EntityRegistry registry, EntityClient entityClient)
      throws IllegalAccessException {
    GmsGraphQLEngineArgs args = new GmsGraphQLEngineArgs();
    GraphQLQueryConfiguration query = new GraphQLQueryConfiguration();
    query.setComplexityLimit(10000);
    query.setDepthLimit(50);
    GraphQLConfiguration graphQLConfiguration = new GraphQLConfiguration();
    graphQLConfiguration.setQuery(query);
    graphQLConfiguration.setMetrics(new GraphQLMetricsConfiguration());
    args.setEntityRegistry(registry);
    args.setSystemOperationContext(
        TestOperationContexts.systemContextNoSearchAuthorization(registry));
    args.setFeatureFlags(new FeatureFlags());
    args.setIngestionConfiguration(new IngestionConfiguration());
    args.setAuthenticationConfiguration(new AuthenticationConfiguration());
    args.setAuthorizationConfiguration(new AuthorizationConfiguration());
    args.setGraphQLConfiguration(graphQLConfiguration);
    args.setEntityClient(entityClient);

    for (var field : GmsGraphQLEngineArgs.class.getDeclaredFields()) {
      Class<?> type = field.getType();
      if (Modifier.isStatic(field.getModifiers())
          || type.isPrimitive()
          || type == String.class
          || Number.class.isAssignableFrom(type)
          || type == Boolean.class) {
        continue;
      }
      field.setAccessible(true);
      if (field.get(args) == null) {
        // Services answer nothing (no default view, no extra facets), as when nothing is stored.
        field.set(args, isConfiguration(type) ? mock(type, RETURNS_DEEP_STUBS) : mock(type));
      }
    }
    return args;
  }

  private static boolean isConfiguration(Class<?> type) {
    return type.getName().startsWith("com.linkedin.metadata.config.")
        || type.getSimpleName().endsWith("Configuration")
        || type.getSimpleName().endsWith("Config")
        || type == FeatureFlags.class
        || type.getSimpleName().equals("GitVersion");
  }
}
