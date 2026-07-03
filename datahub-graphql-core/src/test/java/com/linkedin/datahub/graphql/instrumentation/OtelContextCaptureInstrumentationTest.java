package com.linkedin.datahub.graphql.instrumentation;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import com.linkedin.datahub.graphql.LazyDataLoaderRegistry;
import com.linkedin.datahub.graphql.QueryContext;
import graphql.GraphQLContext;
import graphql.execution.instrumentation.parameters.InstrumentationExecutionParameters;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Scope;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import org.dataloader.DataLoader;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Asserts the OTel context relay used to parent DataLoader batch spans under the GraphQL operation
 * span: the capture instrumentation snapshots Context.current() onto the registry; the registry
 * re-activates that context while invoking the DataLoader supplier; the supplier (in
 * GmsGraphQLEngine) then captures it for use inside the async batch dispatch.
 *
 * <p>Targets the fragile in-process relay (instrumentation -> registry -> supplier). It does not
 * cover ChainedInstrumentation ordering — that contract is owned by graphql-java.
 */
public class OtelContextCaptureInstrumentationTest {

  private SdkTracerProvider tracerProvider;
  private InMemorySpanExporter spanExporter;
  private Tracer tracer;

  @BeforeMethod
  public void setUp() {
    spanExporter = InMemorySpanExporter.create();
    tracerProvider =
        SdkTracerProvider.builder()
            .addSpanProcessor(SimpleSpanProcessor.create(spanExporter))
            .build();
    tracer = tracerProvider.tracerBuilder("test").build();
  }

  @AfterMethod
  public void tearDown() {
    tracerProvider.close();
  }

  @Test
  public void captureInstrumentationRelaysOperationSpanIntoDataLoaderCreation() throws Exception {
    AtomicReference<Span> spanSeenAtSupplierInvocation = new AtomicReference<>();
    Map<String, Function<QueryContext, DataLoader<?, ?>>> suppliers =
        Map.of(
            "test",
            qc -> {
              spanSeenAtSupplierInvocation.set(Span.current());
              return DataLoader.newDataLoader(
                  (keys, ctx) -> CompletableFuture.completedFuture(keys));
            });
    LazyDataLoaderRegistry registry =
        new LazyDataLoaderRegistry(mock(QueryContext.class), suppliers);

    Span operationSpan = tracer.spanBuilder("graphql.operation").startSpan();
    try (Scope ignored = operationSpan.makeCurrent()) {
      GraphQLContext gqlContext =
          GraphQLContext.newContext().of(LazyDataLoaderRegistry.class, registry).build();
      InstrumentationExecutionParameters params = mock(InstrumentationExecutionParameters.class);
      when(params.getGraphQLContext()).thenReturn(gqlContext);
      OtelContextCaptureInstrumentation.INSTANCE.beginExecution(params, null);
    }

    // Pull the DataLoader from a thread with no ambient OTel context — the supplier should still
    // observe the operation span because the registry re-activates the captured context.
    CompletableFuture.runAsync(() -> registry.getDataLoader("test")).get();
    operationSpan.end();

    assertNotNull(spanSeenAtSupplierInvocation.get(), "supplier did not run");
    assertEquals(
        spanSeenAtSupplierInvocation.get().getSpanContext().getSpanId(),
        operationSpan.getSpanContext().getSpanId(),
        "DataLoader supplier ran outside the operation span; relay is broken");

    SpanData exported = spanExporter.getFinishedSpanItems().get(0);
    assertEquals(exported.getName(), "graphql.operation");
  }

  @Test
  public void registryFastPathWhenNoOtelContextCaptured() throws Exception {
    AtomicReference<Span> spanSeenAtSupplierInvocation = new AtomicReference<>();
    Map<String, Function<QueryContext, DataLoader<?, ?>>> suppliers =
        Map.of(
            "test",
            qc -> {
              spanSeenAtSupplierInvocation.set(Span.current());
              return DataLoader.newDataLoader(
                  (keys, ctx) -> CompletableFuture.completedFuture(keys));
            });
    LazyDataLoaderRegistry registry =
        new LazyDataLoaderRegistry(mock(QueryContext.class), suppliers);

    // When the OTel flag is off, OtelContextCaptureInstrumentation never runs and the registry
    // should invoke the supplier without trying to activate any context.
    CompletableFuture.runAsync(() -> registry.getDataLoader("test")).get();

    assertEquals(
        spanSeenAtSupplierInvocation.get().getSpanContext(),
        Span.getInvalid().getSpanContext(),
        "Expected no active span on the fast path");
  }
}
