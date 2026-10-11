package com.linkedin.metadata.system_telemetry;

import com.linkedin.metadata.config.graphql.ResolverSpansConfiguration;
import graphql.execution.instrumentation.InstrumentationState;
import graphql.execution.instrumentation.SimplePerformantInstrumentation;
import graphql.execution.instrumentation.parameters.InstrumentationCreateStateParameters;
import graphql.execution.instrumentation.parameters.InstrumentationFieldFetchParameters;
import graphql.schema.DataFetcher;
import graphql.schema.GraphQLOutputType;
import graphql.schema.GraphQLTypeUtil;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Scope;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Custom graphql-java instrumentation that emits OpenTelemetry spans for a <em>filtered</em> subset
 * of resolver data-fetches, grafted under the operation span created by {@code GraphQLTelemetry}.
 *
 * <p>Unlike the all-or-nothing OTel {@code GraphQLTelemetry} data-fetcher spans, this
 * implementation traces only top-level {@code Query}/{@code Mutation} fields (when {@code
 * topLevelOnly}) and/or an explicit {@code "ParentType.field"} allow-list, so resolver names appear
 * in traces without span explosion on large list responses.
 *
 * <p>This hooks {@link #instrumentDataFetcher} (not {@code beginFieldFetch}) so the resolver span
 * is made current for the synchronous body of the fetcher. Backend client spans (ES / RDS / Kafka)
 * created during that synchronous window — or on a worker thread whose context was propagated from
 * it (e.g. via the OTel Java agent's executor instrumentation, or DataLoader context capture) —
 * nest under the resolver span. Resolvers that dispatch their work to an <em>uninstrumented</em>
 * executor are NOT guaranteed to nest; their backend spans may parent to the operation span
 * instead. The resolver span name and duration are always correct regardless: for async resolvers
 * the returned {@link CompletableFuture} is wrapped and the span is ended in {@code whenComplete},
 * not at dispatch.
 *
 * <p>Performance: {@code instrumentDataFetcher} can run up to ~100k times per request. The
 * trace/skip decision is therefore memoized per {@code "ParentType.field"} key in a {@link
 * ConcurrentHashMap}, so the steady-state per-call cost is one key build plus a map lookup. Skipped
 * fields return the original {@link DataFetcher} unwrapped — no span, no wrapper allocation.
 *
 * <p>Span naming and attributes are intentionally low-cardinality (bounded by the schema). No urns,
 * ids, query text, variables, or list indexes ever appear in span names or attributes.
 */
public class GraphQLOtelResolverInstrumentation extends SimplePerformantInstrumentation {

  /** Tracer instrumentation scope name. */
  public static final String INSTRUMENTATION_NAME = "datahub-graphql-resolver";

  // Attribute keys (low cardinality, bounded by the GraphQL schema).
  static final String ATTR_PARENT_TYPE = "graphql.parent.type";
  static final String ATTR_FIELD_NAME = "graphql.field.name";
  static final String ATTR_RESOLVER_NAME = "graphql.resolver.name";
  static final String ATTR_OPERATION_TYPE = "graphql.operation.type";
  static final String ATTR_OPERATION_NAME = "graphql.operation.name";

  private static final String QUERY_TYPE = "Query";
  private static final String MUTATION_TYPE = "Mutation";

  private final Tracer tracer;
  private final boolean includeTrivialDataFetchers;
  private final boolean topLevelOnly;
  private final Set<String> allowedPaths;

  /**
   * Memoizes the trace/skip decision keyed on the runtime {@code "ParentType.field"} resolver name.
   * Keyed on the resolver name (not the {@link graphql.schema.GraphQLFieldDefinition} identity) so
   * a field definition shared across parent types (e.g. an interface field) is decided per concrete
   * parent type rather than first-execution-wins.
   */
  private final ConcurrentHashMap<String, Boolean> traceDecisionCache = new ConcurrentHashMap<>();

  public GraphQLOtelResolverInstrumentation(
      ResolverSpansConfiguration config, OpenTelemetry openTelemetry) {
    this(config, openTelemetry.getTracer(INSTRUMENTATION_NAME));
  }

  public GraphQLOtelResolverInstrumentation(ResolverSpansConfiguration config, Tracer tracer) {
    this.tracer = tracer;
    this.includeTrivialDataFetchers = config.isIncludeTrivialDataFetchers();
    this.topLevelOnly = config.isTopLevelOnly();
    // Precompute the allow-list into an immutable O(1) lookup set (never regex). Each configured
    // entry is also split on ',' so the list binds correctly whether Spring relaxed-binding yields
    // a List<String> or a single comma-separated string from the env var.
    Set<String> set = new HashSet<>();
    List<String> configured = config.getAllowedPaths();
    if (configured != null) {
      for (String entry : configured) {
        if (entry != null) {
          for (String part : entry.split(",")) {
            String trimmed = part.trim();
            if (!trimmed.isEmpty()) {
              set.add(trimmed);
            }
          }
        }
      }
    }
    this.allowedPaths = Set.copyOf(set);
  }

  /**
   * No per-request state is needed — the trace/skip decision is memoized on the instrumentation
   * instance. graphql-java accepts a {@code null} state for {@link
   * SimplePerformantInstrumentation}.
   */
  @Override
  public InstrumentationState createState(InstrumentationCreateStateParameters parameters) {
    return null;
  }

  @Override
  public DataFetcher<?> instrumentDataFetcher(
      DataFetcher<?> dataFetcher,
      InstrumentationFieldFetchParameters parameters,
      InstrumentationState state) {

    String parentType = parentTypeName(parameters);
    String fieldName = parameters.getField().getName();
    String resolverName = parentType + "." + fieldName;

    // Per-call cost: one key build + map lookup. The decision is computed at most once per distinct
    // resolver name (computeIfAbsent), so trivial-fetcher status is sampled only on the first miss.
    Boolean shouldTrace =
        traceDecisionCache.computeIfAbsent(
            resolverName,
            key -> computeShouldTrace(parentType, key, parameters.isTrivialDataFetcher()));

    if (!Boolean.TRUE.equals(shouldTrace)) {
      // Not traced: return the original fetcher unwrapped. No span, no wrapper allocation.
      return dataFetcher;
    }

    String operationType = operationType(parameters);
    String operationName = operationName(parameters);

    return environment -> {
      Span span =
          tracer
              .spanBuilder(resolverName)
              .setAttribute(ATTR_PARENT_TYPE, parentType)
              .setAttribute(ATTR_FIELD_NAME, fieldName)
              .setAttribute(ATTR_RESOLVER_NAME, resolverName)
              .setAttribute(ATTR_OPERATION_TYPE, operationType)
              .setAttribute(ATTR_OPERATION_NAME, operationName)
              .startSpan();

      // Keep the resolver span current across the synchronous body of the fetcher, so backend
      // client spans (ES / RDS / Kafka) created here — or on a worker thread carrying propagated
      // context — nest under this resolver span.
      try (Scope ignored = span.makeCurrent()) {
        Object result = dataFetcher.get(environment);

        if (result instanceof CompletableFuture) {
          // Async: end the span when the future completes (accurate resolver duration), not at
          // dispatch. The span is intentionally no longer current on this thread by then.
          return ((CompletableFuture<?>) result)
              .whenComplete(
                  (r, t) -> {
                    if (t != null) {
                      span.recordException(t);
                      span.setStatus(StatusCode.ERROR);
                    }
                    span.end();
                  });
        }

        // Synchronous result: end immediately.
        span.end();
        return result;

      } catch (Throwable t) {
        // Throwable, not just Exception — otherwise an Error would skip span.end() and leak the
        // span in the SDK store. Rethrow with checked casts (no sneaky-throw): DataFetcher.get
        // already declares `throws Exception`, so the final cast compiles.
        span.recordException(t);
        span.setStatus(StatusCode.ERROR);
        span.end();
        if (t instanceof Error) {
          throw (Error) t;
        }
        if (t instanceof RuntimeException) {
          throw (RuntimeException) t;
        }
        throw (Exception) t;
      }
    };
  }

  /**
   * Computes the trace/skip decision for a resolver. Called at most once per distinct resolver name
   * via {@code computeIfAbsent}.
   */
  private boolean computeShouldTrace(String parentType, String resolverName, boolean trivial) {
    boolean shouldTrace =
        allowedPaths.contains(resolverName)
            || (topLevelOnly
                && (QUERY_TYPE.equals(parentType) || MUTATION_TYPE.equals(parentType)));

    if (!includeTrivialDataFetchers && trivial) {
      shouldTrace = false;
    }
    return shouldTrace;
  }

  private static String parentTypeName(InstrumentationFieldFetchParameters parameters) {
    GraphQLOutputType parentType = parameters.getExecutionStepInfo().getParent().getType();
    return GraphQLTypeUtil.simplePrint(parentType);
  }

  private static String operationType(InstrumentationFieldFetchParameters parameters) {
    if (parameters.getExecutionContext() != null
        && parameters.getExecutionContext().getOperationDefinition() != null
        && parameters.getExecutionContext().getOperationDefinition().getOperation() != null) {
      return parameters
          .getExecutionContext()
          .getOperationDefinition()
          .getOperation()
          .name()
          .toLowerCase();
    }
    return "query";
  }

  private static String operationName(InstrumentationFieldFetchParameters parameters) {
    if (parameters.getExecutionContext() != null
        && parameters.getExecutionContext().getOperationDefinition() != null
        && parameters.getExecutionContext().getOperationDefinition().getName() != null) {
      return parameters.getExecutionContext().getOperationDefinition().getName();
    }
    return "unnamed";
  }
}
