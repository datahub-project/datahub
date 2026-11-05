package com.linkedin.datahub.graphql.instrumentation;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.linkedin.datahub.graphql.QueryContext;
import graphql.execution.ExecutionStepInfo;
import graphql.execution.instrumentation.InstrumentationState;
import graphql.execution.instrumentation.SimplePerformantInstrumentation;
import graphql.execution.instrumentation.parameters.InstrumentationFieldFetchParameters;
import graphql.schema.DataFetcher;
import graphql.schema.GraphQLFieldDefinition;
import java.time.Duration;
import lombok.extern.slf4j.Slf4j;

/**
 * Logs a WARN when a GraphQL field marked {@code @deprecated} is called, at most once per field and
 * actor per hour, so a field's remaining callers can be found before it is removed.
 */
@Slf4j
public class DeprecatedFieldUsageInstrumentation extends SimplePerformantInstrumentation {

  private final Cache<String, Boolean> recentlyLogged =
      Caffeine.newBuilder().maximumSize(10_000).expireAfterWrite(Duration.ofHours(1)).build();

  @Override
  public DataFetcher<?> instrumentDataFetcher(
      DataFetcher<?> dataFetcher,
      InstrumentationFieldFetchParameters parameters,
      InstrumentationState state) {
    final ExecutionStepInfo stepInfo = parameters.getExecutionStepInfo();
    final GraphQLFieldDefinition field = stepInfo.getFieldDefinition();
    if (field.isDeprecated()) {
      final String coordinate = stepInfo.getObjectType().getName() + "." + field.getName();
      final QueryContext context =
          parameters.getEnvironment().getGraphQlContext().get(QueryContext.class);
      final String actor = context != null ? context.getActorUrn() : "unknown";
      if (recentlyLogged.asMap().putIfAbsent(coordinate + "|" + actor, Boolean.TRUE) == null) {
        log.warn("Deprecated GraphQL field {} was called by {}", coordinate, actor);
      }
    }
    return dataFetcher;
  }
}
