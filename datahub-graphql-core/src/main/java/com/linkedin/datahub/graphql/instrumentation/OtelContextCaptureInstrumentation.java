package com.linkedin.datahub.graphql.instrumentation;

import com.linkedin.datahub.graphql.LazyDataLoaderRegistry;
import graphql.ExecutionResult;
import graphql.execution.instrumentation.InstrumentationContext;
import graphql.execution.instrumentation.InstrumentationState;
import graphql.execution.instrumentation.SimpleInstrumentationContext;
import graphql.execution.instrumentation.SimplePerformantInstrumentation;
import graphql.execution.instrumentation.parameters.InstrumentationExecutionParameters;
import io.opentelemetry.context.Context;

/**
 * Captures the active OTel {@link Context} at the start of each GraphQL execution (inside the
 * operation span created by GraphQLTelemetry) and stores it on the {@link LazyDataLoaderRegistry}.
 * The registry then makes this context current when lazily creating DataLoaders, so that batch
 * functions (e.g. getEntitiesV2) are parented to the operation span rather than to whichever field
 * resolver or dispatch thread happens to be active at batch-dispatch time.
 *
 * <p>Stateless — use {@link #INSTANCE}.
 */
public final class OtelContextCaptureInstrumentation extends SimplePerformantInstrumentation {

  public static final OtelContextCaptureInstrumentation INSTANCE =
      new OtelContextCaptureInstrumentation();

  private OtelContextCaptureInstrumentation() {}

  @Override
  public InstrumentationContext<ExecutionResult> beginExecution(
      InstrumentationExecutionParameters params, InstrumentationState state) {
    LazyDataLoaderRegistry registry = params.getGraphQLContext().get(LazyDataLoaderRegistry.class);
    if (registry != null) {
      registry.setOtelContext(Context.current());
    }
    return SimpleInstrumentationContext.noOp();
  }
}
