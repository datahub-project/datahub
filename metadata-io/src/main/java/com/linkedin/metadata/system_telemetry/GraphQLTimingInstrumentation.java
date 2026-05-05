package com.linkedin.metadata.system_telemetry;

import com.datahub.authentication.Actor;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.metadata.config.graphql.GraphQLMetricsConfiguration;
import com.linkedin.metadata.config.graphql.GraphQLShapeLoggingConfiguration;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import graphql.ExecutionResult;
import graphql.execution.instrumentation.InstrumentationContext;
import graphql.execution.instrumentation.InstrumentationState;
import graphql.execution.instrumentation.SimpleInstrumentationContext;
import graphql.execution.instrumentation.SimplePerformantInstrumentation;
import graphql.execution.instrumentation.parameters.InstrumentationCreateStateParameters;
import graphql.execution.instrumentation.parameters.InstrumentationExecuteOperationParameters;
import graphql.execution.instrumentation.parameters.InstrumentationExecutionParameters;
import graphql.execution.instrumentation.parameters.InstrumentationFieldFetchParameters;
import graphql.language.Document;
import graphql.schema.DataFetcher;
import graphql.schema.GraphQLOutputType;
import graphql.schema.GraphQLTypeUtil;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GraphQLTimingInstrumentation extends SimplePerformantInstrumentation {

  private final MeterRegistry meterRegistry;
  private final GraphQLMetricsConfiguration config;
  private final Set<String> fieldInstrumentedOperations;
  private final List<PathMatcher> fieldInstrumentedPaths;
  private final double[] percentiles;

  @Nullable private final GraphQLShapeLoggingConfiguration shapeLoggingConfig;

  /** Dedicated logger for shape log entries — inherits the ASYNC_GRAPHQL_DEBUG_FILE appender. */
  private static final Logger SHAPE_LOG = LoggerFactory.getLogger("com.datahub.graphql.shape");

  /** Context map key for actor identity. */
  public static final String GRAPHQL_CONTEXT_ACTOR_KEY = "actor";

  /** Used only for serialising shape log payloads. ObjectMapper is thread-safe and stateless. */
  private static final ObjectMapper shapeObjectMapper = new ObjectMapper();

  // Inner class for path matching
  private static class PathMatcher {
    private final String pattern;
    private final Pattern regex;

    PathMatcher(String pattern) {
      this.pattern = pattern;
      this.regex = Pattern.compile(convertPathToRegex(pattern));
    }

    boolean matches(String path) {
      return regex.matcher(path).matches();
    }

    private static String convertPathToRegex(String pattern) {
      // Convert GraphQL path patterns to regex
      // Examples:
      // /user -> matches exactly /user
      // /user/* -> matches /user/name, /user/id but not /user/posts/0
      // /user/** -> matches /user and anything under it at any depth
      // /*/comments/* -> matches /posts/comments/text, /articles/comments/author
      // /user/posts/*/comments -> matches /user/posts/0/comments, /user/posts/1/comments

      String regex =
          pattern
              .replace(".", "\\.") // Escape dots
              .replace("/**", "(/.*)?") // /** matches current and any descendants
              .replace("/*", "/[^/]+") // /* matches exactly one segment
              .replaceAll("/\\*\\*$", "(/.*)?"); // /** at end is optional

      // For patterns ending with /**, also match the parent path
      if (pattern.endsWith("/**")) {
        String parentPath = pattern.substring(0, pattern.length() - 3);
        regex = "(" + parentPath + "|" + regex + ")";
      }

      return "^" + regex + "$";
    }
  }

  /**
   * Full constructor.
   *
   * @param meterRegistry Micrometer registry for metrics
   * @param config GraphQL metrics configuration
   * @param shapeLoggingConfig optional shape logging configuration; pass {@code null} to disable
   */
  public GraphQLTimingInstrumentation(
      MeterRegistry meterRegistry,
      GraphQLMetricsConfiguration config,
      @Nullable GraphQLShapeLoggingConfiguration shapeLoggingConfig) {
    this.meterRegistry = meterRegistry;
    this.config = config;
    this.fieldInstrumentedOperations = parseOperationsList(config.getFieldLevelOperations());
    this.fieldInstrumentedPaths = parsePathPatterns(config.getFieldLevelPaths());
    this.percentiles = MetricUtils.parsePercentiles(config.getPercentiles());
    this.shapeLoggingConfig = shapeLoggingConfig;
  }

  /**
   * Backward-compatible constructor — shape logging is disabled.
   *
   * @param meterRegistry Micrometer registry for metrics
   * @param config GraphQL metrics configuration
   */
  public GraphQLTimingInstrumentation(
      MeterRegistry meterRegistry, GraphQLMetricsConfiguration config) {
    this(meterRegistry, config, null);
  }

  private Set<String> parseOperationsList(String operationsList) {
    if (operationsList == null || operationsList.trim().isEmpty()) {
      return Collections.emptySet();
    }

    Set<String> operations = new HashSet<>();
    for (String operation : operationsList.split(",")) {
      String trimmed = operation.trim();
      if (!trimmed.isEmpty()) {
        operations.add(trimmed);
      }
    }
    return operations;
  }

  private List<PathMatcher> parsePathPatterns(String pathsList) {
    if (pathsList == null || pathsList.trim().isEmpty()) {
      return Collections.emptyList();
    }

    List<PathMatcher> matchers = new ArrayList<>();
    for (String path : pathsList.split(",")) {
      String trimmed = path.trim();
      if (!trimmed.isEmpty()) {
        matchers.add(new PathMatcher(trimmed));
      }
    }
    return matchers;
  }

  @Override
  public InstrumentationState createState(InstrumentationCreateStateParameters parameters) {
    return new TimingState();
  }

  @Override
  public InstrumentationContext<ExecutionResult> beginExecution(
      InstrumentationExecutionParameters parameters, InstrumentationState state) {

    TimingState timingState = (TimingState) state;
    timingState.startTime = System.nanoTime();

    // Determine operation name and filtering mode
    String operationName = getOperationName(parameters);
    timingState.operationName = operationName;
    timingState.filteringMode = determineFilteringMode(operationName);

    // Extract actorUrn for caller attribution (best-effort)
    try {
      Object actorContext = parameters.getGraphQLContext().get(GRAPHQL_CONTEXT_ACTOR_KEY);
      if (actorContext instanceof Actor actor) {
        timingState.actorUrn = actor.toUrnStr();
      }
    } catch (Exception innerEx) {
      // ActorUrn extraction is optional — log at debug (not error, to avoid flooding error logs)
      SHAPE_LOG.debug("Could not extract actorUrn from ExecutionContext", innerEx);
    }
    return SimpleInstrumentationContext.whenCompleted(
        (result, t) -> {
          long duration = System.nanoTime() - timingState.startTime;

          // Use AST-based operation type from queryShape if available (more accurate than string
          // prefix)
          String operationType =
              timingState.queryShape != null
                  ? timingState.queryShape.getOperationType()
                  : getOperationType(parameters);

          Timer.builder("graphql.request.duration")
              .tag("operation", operationName)
              .tag("operation.type", operationType)
              .tag("success", String.valueOf(t == null && result.getErrors().isEmpty()))
              .tag("field.filtering", timingState.filteringMode.toString())
              .register(meterRegistry)
              .record(duration, TimeUnit.NANOSECONDS);

          // Record error count
          if (t != null || !result.getErrors().isEmpty()) {
            meterRegistry
                .counter(
                    "graphql.request.errors",
                    "operation",
                    operationName,
                    "operation.type",
                    operationType)
                .increment();
          }

          // Record fields instrumented count
          if (timingState.fieldsInstrumented > 0) {
            meterRegistry
                .counter(
                    "graphql.fields.instrumented",
                    "operation",
                    operationName,
                    "filtering.mode",
                    timingState.filteringMode.toString())
                .increment(timingState.fieldsInstrumented);
          }

          // Shape logging (threshold-based, best-effort)
          if (shapeLoggingConfig != null
              && shapeLoggingConfig.isEnabled()
              && timingState.queryShape != null) {
            long durationMs = TimeUnit.NANOSECONDS.toMillis(duration);
            evaluateAndLogShape(timingState, result, durationMs);
          }
        });
  }

  @Override
  public InstrumentationContext<ExecutionResult> beginExecuteOperation(
      InstrumentationExecuteOperationParameters parameters, InstrumentationState state) {

    if (shapeLoggingConfig == null || !shapeLoggingConfig.isEnabled()) {
      return SimpleInstrumentationContext.noOp();
    }

    TimingState timingState = (TimingState) state;
    try {
      Document document = parameters.getExecutionContext().getDocument();
      timingState.queryShape = QueryShapeAnalyzer.analyze(document, timingState.operationName);
      // Always-on cheap shape metrics — emitted regardless of thresholds
      emitShapeMetrics(timingState.queryShape);
    } catch (Exception e) {
      // Shape analysis is best-effort — never propagate
      SHAPE_LOG.debug("Shape analysis failed", e);
    }
    return SimpleInstrumentationContext.noOp();
  }

  enum FilteringMode {
    DISABLED, // Field-level metrics disabled globally
    ALL_FIELDS, // No filtering, instrument all fields
    BY_OPERATION, // Filter by operation name only
    BY_PATH, // Filter by path pattern only
    BY_BOTH // Filter by both operation and path
  }

  private FilteringMode determineFilteringMode(String operationName) {
    if (!config.isFieldLevelEnabled()) {
      return FilteringMode.DISABLED;
    }

    boolean hasOperationFilter = !fieldInstrumentedOperations.isEmpty();
    boolean hasPathFilter = !fieldInstrumentedPaths.isEmpty();

    if (!hasOperationFilter && !hasPathFilter) {
      return FilteringMode.ALL_FIELDS;
    } else if (hasOperationFilter && !hasPathFilter) {
      return FilteringMode.BY_OPERATION;
    } else if (!hasOperationFilter && hasPathFilter) {
      return FilteringMode.BY_PATH;
    } else {
      return FilteringMode.BY_BOTH;
    }
  }

  private boolean shouldInstrumentField(
      String operationName, String fieldPath, FilteringMode mode) {
    switch (mode) {
      case DISABLED:
        return false;

      case ALL_FIELDS:
        return true;

      case BY_OPERATION:
        return fieldInstrumentedOperations.contains(operationName);

      case BY_PATH:
        return matchesAnyPath(fieldPath);

      case BY_BOTH:
        // Both conditions must be met
        return fieldInstrumentedOperations.contains(operationName) && matchesAnyPath(fieldPath);

      default:
        return false;
    }
  }

  private boolean matchesAnyPath(String fieldPath) {
    for (PathMatcher matcher : fieldInstrumentedPaths) {
      if (matcher.matches(fieldPath)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public DataFetcher<?> instrumentDataFetcher(
      DataFetcher<?> dataFetcher,
      InstrumentationFieldFetchParameters parameters,
      InstrumentationState state) {

    TimingState timingState = (TimingState) state;

    String fieldPath = parameters.getExecutionStepInfo().getPath().toString();

    if (!shouldInstrumentField(timingState.operationName, fieldPath, timingState.filteringMode)) {
      return dataFetcher;
    }

    if (parameters.isTrivialDataFetcher() && !config.isTrivialDataFetchersEnabled()) {
      return dataFetcher;
    }

    timingState.fieldsInstrumented++;

    return environment -> {
      GraphQLOutputType parentType = parameters.getExecutionStepInfo().getParent().getType();
      String parentTypeName = GraphQLTypeUtil.simplePrint(parentType);
      String fieldName = parameters.getExecutionStepInfo().getFieldDefinition().getName();

      long startTime = System.nanoTime();
      try {
        Object result = dataFetcher.get(environment);

        if (result instanceof CompletableFuture) {
          CompletableFuture<?> cf = (CompletableFuture<?>) result;
          return cf.whenComplete(
              (r, t) -> {
                long duration = System.nanoTime() - startTime;
                recordFieldMetrics(
                    parentTypeName,
                    fieldName,
                    fieldPath,
                    duration,
                    t == null,
                    timingState.operationName);
              });
        }

        long duration = System.nanoTime() - startTime;
        recordFieldMetrics(
            parentTypeName, fieldName, fieldPath, duration, true, timingState.operationName);
        return result;

      } catch (Exception e) {
        long duration = System.nanoTime() - startTime;
        recordFieldMetrics(
            parentTypeName, fieldName, fieldPath, duration, false, timingState.operationName);
        throw e;
      }
    };
  }

  private String getOperationName(InstrumentationExecutionParameters parameters) {
    if (parameters.getOperation() != null) {
      return parameters.getOperation();
    }
    if (parameters.getExecutionInput() != null
        && parameters.getExecutionInput().getOperationName() != null) {
      return parameters.getExecutionInput().getOperationName();
    }
    return "unnamed";
  }

  private String getOperationType(InstrumentationExecutionParameters parameters) {
    if (parameters.getExecutionInput() != null) {
      String query = parameters.getExecutionInput().getQuery();
      if (query != null) {
        String trimmed = query.trim();
        if (trimmed.startsWith("mutation")) return "mutation";
        if (trimmed.startsWith("subscription")) return "subscription";
      }
    }
    return "query";
  }

  private void recordFieldMetrics(
      String parentType,
      String fieldName,
      String fieldPath,
      long duration,
      boolean success,
      String operationName) {
    Timer.Builder timerBuilder =
        Timer.builder("graphql.field.duration")
            .tag("parent.type", parentType)
            .tag("field", fieldName)
            .tag("operation", operationName)
            .tag("success", String.valueOf(success));

    if (config.isFieldLevelPathEnabled()) {
      timerBuilder.tag("path", truncatePath(fieldPath));
    }

    timerBuilder
        .publishPercentiles(percentiles)
        .register(meterRegistry)
        .record(duration, TimeUnit.NANOSECONDS);

    // Record field errors
    if (!success) {
      meterRegistry
          .counter(
              "graphql.field.errors",
              "parent.type",
              parentType,
              "field",
              fieldName,
              "operation",
              operationName)
          .increment();
    }
  }

  private String truncatePath(String path) {
    // Truncate paths to avoid high cardinality
    // Handle both formats:
    // - Array indices with brackets: /searchResults[0]/entity -> /searchResults[*]/entity
    // - Array indices with slashes: /users/0/name -> /users/*/name
    return path.replaceAll("\\[\\d+\\]", "[*]") // Replace [0], [1], etc. with [*]
        .replaceAll("/\\d+", "/*"); // Replace /0, /1, etc. with /*
  }

  /**
   * Evaluates all configured thresholds against the completed request and emits a structured JSON
   * log entry at DEBUG level when at least one threshold is crossed.
   *
   * <p>Response shape analysis (the more expensive part) is deferred until at least one threshold
   * has already been exceeded.
   */
  private void evaluateAndLogShape(TimingState state, ExecutionResult result, long durationMs) {
    try {
      QueryShapeAnalyzer.QueryShape qs = state.queryShape;

      // Early exit: check cheap thresholds first (no response analysis)
      boolean fieldCountCrossed = qs.getFieldCount() >= shapeLoggingConfig.getFieldCountThreshold();
      boolean durationCrossed = durationMs >= shapeLoggingConfig.getDurationThresholdMs();
      boolean errorCountCrossed =
          result.getErrors().size() >= shapeLoggingConfig.getErrorCountThreshold();

      if (!fieldCountCrossed && !durationCrossed && !errorCountCrossed) {
        return; // No threshold crossed, no allocation
      }

      // Expensive: analyze response only if a threshold already crossed
      ResponseShapeAnalyzer.ResponseShape responseShape = ResponseShapeAnalyzer.analyze(result);
      long responseBytesEstimate =
          (long) responseShape.getFieldCount() * GraphQLShapeConstants.BYTES_PER_FIELD_ESTIMATE;
      boolean responseSizeCrossed =
          responseBytesEstimate >= shapeLoggingConfig.getResponseSizeThresholdBytes();

      // Build crossed list (pre-allocate, max 4 items)
      List<String> crossed = new ArrayList<>(4);
      if (fieldCountCrossed) crossed.add("field_count");
      if (durationCrossed) crossed.add("duration");
      if (errorCountCrossed) crossed.add("error_count");
      if (responseSizeCrossed) crossed.add("response_size");

      // Build payload (pre-allocate with known capacity for better cache locality)
      Map<String, Object> payload = new LinkedHashMap<>(15);

      // Operation metadata
      payload.put("operation", state.operationName);
      payload.put("operationType", qs.getOperationType());
      payload.put("topLevelResolvers", qs.getTopLevelFields());
      if (state.actorUrn != null) {
        payload.put("actorUrn", state.actorUrn);
      }

      // Timing & size
      payload.put("durationMs", durationMs);
      payload.put("responseBytesEstimate", responseBytesEstimate);

      // Request structure (query shape)
      payload.put("requestQueryShape", qs.getNormalizedShape());
      payload.put("requestQueryShapeHash", qs.getShapeHash());
      payload.put("requestFieldCount", qs.getFieldCount());
      payload.put("requestMaxDepth", qs.getMaxDepth());

      // Response structure (actual response shape)
      payload.put("responseNormalizedShape", responseShape.getNormalizedShape());
      payload.put("responseFieldCount", responseShape.getFieldCount());

      // Heavy fields: arrays with their sizes (extracted during AST traversal)
      if (!responseShape.getHeavyFields().isEmpty()) {
        List<Map<String, Object>> heavyFieldsList = new ArrayList<>();
        for (ResponseShapeAnalyzer.HeavyField hf : responseShape.getHeavyFields()) {
          Map<String, Object> entry = new LinkedHashMap<>(2);
          entry.put("path", hf.path());
          entry.put("size", hf.size());
          heavyFieldsList.add(entry);
        }
        payload.put("heavyFields", heavyFieldsList);
      }

      // Diagnostic info
      payload.put("thresholdsCrossed", crossed);
      payload.put("errorCount", result.getErrors().size());
      payload.put("timestamp", Instant.now().toString());

      SHAPE_LOG.info(shapeObjectMapper.writeValueAsString(payload));
    } catch (Exception e) {
      // Never propagate shape logging errors — best-effort only
      SHAPE_LOG.error("Shape log evaluation failed", e);
    }
  }

  /**
   * Emits always-on (cheap) Micrometer metrics for the query shape. Called unconditionally when
   * shape logging is enabled, regardless of threshold evaluation.
   *
   * <p>Metrics emitted:
   *
   * <ul>
   *   <li>{@code graphql.shape.requests.total} — counter tagged with top-level fields and operation
   *       type
   *   <li>{@code graphql.shape.field_count} — summary of request field counts (request complexity)
   *   <li>{@code graphql.shape.max_depth} — summary of request max depth (nesting complexity)
   * </ul>
   */
  private void emitShapeMetrics(QueryShapeAnalyzer.QueryShape qs) {
    meterRegistry
        .counter(
            "graphql.shape.requests.total",
            "top_level_fields",
            qs.getTopLevelFields(),
            "operation_type",
            qs.getOperationType())
        .increment();

    meterRegistry.summary("graphql.shape.field_count").record(qs.getFieldCount());
    meterRegistry.summary("graphql.shape.max_depth").record(qs.getMaxDepth());
  }

  static class TimingState implements InstrumentationState {
    long startTime;
    String operationName;
    FilteringMode filteringMode;
    int fieldsInstrumented = 0;

    /** Set during {@code beginExecuteOperation}; null until shape logging fires. */
    QueryShapeAnalyzer.QueryShape queryShape;

    /**
     * Actor URN extracted from ExecutionContext; null if unavailable. Used in logs for caller
     * attribution.
     */
    @Nullable String actorUrn;
  }
}
