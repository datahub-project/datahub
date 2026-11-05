package com.linkedin.datahub.graphql.instrumentation;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.linkedin.datahub.graphql.QueryContext;
import graphql.ExecutionResult;
import graphql.execution.ExecutionStepInfo;
import graphql.execution.instrumentation.InstrumentationContext;
import graphql.execution.instrumentation.InstrumentationState;
import graphql.execution.instrumentation.SimpleInstrumentationContext;
import graphql.execution.instrumentation.SimplePerformantInstrumentation;
import graphql.execution.instrumentation.parameters.InstrumentationCreateStateParameters;
import graphql.execution.instrumentation.parameters.InstrumentationExecutionParameters;
import graphql.execution.instrumentation.parameters.InstrumentationFieldFetchParameters;
import graphql.schema.DataFetcher;
import graphql.schema.GraphQLFieldDefinition;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * Records calls to GraphQL fields marked {@code @deprecated}, so that a field's remaining callers
 * can be found before it is removed.
 *
 * <p>Each request counts every deprecated field it resolves once, in the counter {@value
 * #METRIC_NAME} tagged with the field coordinate (for example {@code Query.browse} or {@code
 * Dataset.browsePaths}). The tag values are bounded by the number of deprecated fields in the
 * schema.
 *
 * <p>It also logs a WARN line naming the actor, operation and user agent, at most once per field,
 * operation and actor within {@link #LOG_WINDOW}. Scripts usually send unnamed operations, so the
 * actor and user agent are what identify them.
 *
 * <p>Fields that are not deprecated cost one boolean check per fetch.
 */
@Slf4j
public class DeprecatedFieldUsageInstrumentation extends SimplePerformantInstrumentation {

  static final String METRIC_NAME = "graphql.deprecated.field.requests";
  static final String FIELD_TAG = "field";
  static final Duration LOG_WINDOW = Duration.ofHours(24);
  private static final int MAX_LOGGED_KEYS = 10_000;
  private static final String UNNAMED_OPERATION = "unnamed";
  private static final String UNKNOWN = "unknown";

  @Nullable private final MeterRegistry meterRegistry;
  private final Cache<String, Boolean> recentlyLogged =
      Caffeine.newBuilder().maximumSize(MAX_LOGGED_KEYS).expireAfterWrite(LOG_WINDOW).build();

  public DeprecatedFieldUsageInstrumentation(@Nullable final MeterRegistry meterRegistry) {
    this.meterRegistry = meterRegistry;
  }

  @Override
  public InstrumentationState createState(InstrumentationCreateStateParameters parameters) {
    return new UsageState();
  }

  @Override
  public InstrumentationContext<ExecutionResult> beginExecution(
      InstrumentationExecutionParameters parameters, InstrumentationState state) {
    if (state instanceof UsageState usageState) {
      usageState.operationName =
          parameters.getOperation() != null ? parameters.getOperation() : UNNAMED_OPERATION;
      final QueryContext context = parameters.getGraphQLContext().get(QueryContext.class);
      if (context != null) {
        usageState.actorUrn = context.getActorUrn();
        usageState.userAgent = userAgent(context);
      }
    }
    return SimpleInstrumentationContext.noOp();
  }

  @Override
  public DataFetcher<?> instrumentDataFetcher(
      DataFetcher<?> dataFetcher,
      InstrumentationFieldFetchParameters parameters,
      InstrumentationState state) {
    final ExecutionStepInfo stepInfo = parameters.getExecutionStepInfo();
    final GraphQLFieldDefinition fieldDefinition = stepInfo.getFieldDefinition();
    if (!fieldDefinition.isDeprecated() || !(state instanceof UsageState usageState)) {
      return dataFetcher;
    }
    final String coordinate = stepInfo.getObjectType().getName() + "." + fieldDefinition.getName();
    // A deprecated field inside a list is fetched once per item; count it once per request.
    if (usageState.seenFields.add(coordinate)) {
      recordUsage(usageState, coordinate, fieldDefinition.getDeprecationReason());
    }
    return dataFetcher;
  }

  private void recordUsage(
      @Nonnull final UsageState state,
      @Nonnull final String coordinate,
      @Nullable final String deprecationReason) {
    if (meterRegistry != null) {
      meterRegistry.counter(METRIC_NAME, FIELD_TAG, coordinate).increment();
    }
    final String logKey = coordinate + "|" + state.operationName + "|" + state.actorUrn;
    if (recentlyLogged.asMap().putIfAbsent(logKey, Boolean.TRUE) == null) {
      log.warn(
          "Deprecated GraphQL field {} was called by {} (operation: {}, user agent: {}). {}",
          coordinate,
          state.actorUrn,
          state.operationName,
          state.userAgent,
          deprecationReason);
    }
  }

  @Nonnull
  private static String userAgent(@Nonnull final QueryContext context) {
    try {
      if (context.getOperationContext() != null
          && context.getOperationContext().getRequestContext() != null) {
        return context.getOperationContext().getRequestContext().getUserAgent();
      }
    } catch (RuntimeException e) {
      // The user agent only enriches the log line; never fail the request for it.
      log.debug("Could not read the user agent for deprecated field logging", e);
    }
    return UNKNOWN;
  }

  static class UsageState implements InstrumentationState {
    private final Set<String> seenFields = ConcurrentHashMap.newKeySet();
    private String operationName = UNNAMED_OPERATION;
    private String actorUrn = UNKNOWN;
    private String userAgent = UNKNOWN;
  }
}
