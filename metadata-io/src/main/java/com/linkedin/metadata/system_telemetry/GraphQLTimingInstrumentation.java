package com.linkedin.metadata.system_telemetry;

import com.linkedin.metadata.config.graphql.GraphQLMetricsConfiguration;
import graphql.ExecutionResult;
import graphql.execution.instrumentation.InstrumentationContext;
import graphql.execution.instrumentation.InstrumentationState;
import graphql.execution.instrumentation.SimpleInstrumentationContext;
import graphql.execution.instrumentation.SimplePerformantInstrumentation;
import graphql.execution.instrumentation.parameters.InstrumentationCreateStateParameters;
import graphql.execution.instrumentation.parameters.InstrumentationExecutionParameters;
import graphql.execution.instrumentation.parameters.InstrumentationFieldFetchParameters;
import graphql.schema.DataFetcher;
import graphql.schema.GraphQLOutputType;
import graphql.schema.GraphQLTypeUtil;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

public class GraphQLTimingInstrumentation extends SimplePerformantInstrumentation {

  private final MeterRegistry meterRegistry;
  private final GraphQLMetricsConfiguration config;
  private final Set<String> fieldInstrumentedOperations;
  private final List<PathMatcher> fieldInstrumentedPaths;
  private final double[] percentiles;

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

  public GraphQLTimingInstrumentation(
      MeterRegistry meterRegistry, GraphQLMetricsConfiguration config) {
    this.meterRegistry = meterRegistry;
    this.config = config;
    this.fieldInstrumentedOperations = parseOperationsList(config.getFieldLevelOperations());
    this.fieldInstrumentedPaths = parsePathPatterns(config.getFieldLevelPaths());
    this.percentiles = parsePercentiles(config.getPercentiles());
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

    return SimpleInstrumentationContext.whenCompleted(
        (result, t) -> {
          long duration = System.nanoTime() - timingState.startTime;

          Timer.builder("graphql.request.duration")
              .tag("operation", operationName)
              .tag("operation.type", getOperationType(parameters))
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
                    getOperationType(parameters))
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
        });
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

    // Get the field path
    String fieldPath = parameters.getExecutionStepInfo().getPath().toString();

    // Check if we should instrument this field
    if (!shouldInstrumentField(timingState.operationName, fieldPath, timingState.filteringMode)) {
      return dataFetcher;
    }

    // Skip trivial data fetchers unless explicitly included
    if (parameters.isTrivialDataFetcher() && !config.isTrivialDataFetchersEnabled()) {
      return dataFetcher;
    }

    // Increment counter of instrumented fields
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
        if (query.trim().startsWith("mutation")) return "mutation";
        if (query.trim().startsWith("subscription")) return "subscription";
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

  private double[] parsePercentiles(String percentilesConfig) {
    if (percentilesConfig == null || percentilesConfig.trim().isEmpty()) {
      // Default percentiles
      return new double[] {0.5, 0.95, 0.99};
    }

    String[] parts = percentilesConfig.split(",");
    double[] result = new double[parts.length];
    for (int i = 0; i < parts.length; i++) {
      result[i] = Double.parseDouble(parts[i].trim());
    }
    return result;
  }

  static class TimingState implements InstrumentationState {
    long startTime;
    String operationName;
    FilteringMode filteringMode;
    int fieldsInstrumented = 0;
  }
}
