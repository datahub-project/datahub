package com.linkedin.datahub.graphql.instrumentation;

import graphql.ExecutionResult;
import graphql.execution.MergedSelectionSet;
import graphql.execution.instrumentation.ExecutionStrategyInstrumentationContext;
import graphql.execution.instrumentation.InstrumentationState;
import graphql.execution.instrumentation.SimplePerformantInstrumentation;
import graphql.execution.instrumentation.parameters.InstrumentationExecutionParameters;
import graphql.execution.instrumentation.parameters.InstrumentationExecutionStrategyParameters;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Adds the name of the first top-level field of the operation to the result extensions, under
 * {@link #EXTENSION_KEY}. Metrics use this name as a tag. The value is null if the operation does
 * not start, for example if validation fails. Remove the extension before you send the response.
 *
 * <p>graphql-java calls {@code beginExecutionStrategy} one time for each operation, for the
 * top-level fields only. At that point, graphql-java has merged the fields, expanded the fragments
 * and applied {@code @skip} and {@code @include}.
 *
 * <p>This class has no state. Use {@link #INSTANCE}.
 */
public final class RootFieldNameInstrumentation extends SimplePerformantInstrumentation {

  public static final String EXTENSION_KEY = "rootFieldName";

  public static final RootFieldNameInstrumentation INSTANCE = new RootFieldNameInstrumentation();

  private RootFieldNameInstrumentation() {}

  @Override
  public ExecutionStrategyInstrumentationContext beginExecutionStrategy(
      InstrumentationExecutionStrategyParameters parameters, InstrumentationState state) {
    MergedSelectionSet rootFields = parameters.getExecutionStrategyParameters().getFields();
    if (!rootFields.isEmpty()) {
      parameters
          .getExecutionContext()
          .getGraphQLContext()
          .put(EXTENSION_KEY, rootFields.getSubFieldsList().get(0).getName());
    }
    return ExecutionStrategyInstrumentationContext.NOOP;
  }

  @Override
  public CompletableFuture<ExecutionResult> instrumentExecutionResult(
      ExecutionResult executionResult,
      InstrumentationExecutionParameters parameters,
      InstrumentationState state) {
    Map<Object, Object> extensions =
        executionResult.getExtensions() == null
            ? new LinkedHashMap<>()
            : new LinkedHashMap<>(executionResult.getExtensions());
    extensions.put(EXTENSION_KEY, parameters.getGraphQLContext().get(EXTENSION_KEY));
    return CompletableFuture.completedFuture(
        executionResult.transform(builder -> builder.extensions(extensions)));
  }
}
