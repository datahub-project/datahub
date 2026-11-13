package com.linkedin.metadata.system_telemetry;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.metadata.config.graphql.ResolverSpansConfiguration;
import graphql.execution.ExecutionStepInfo;
import graphql.execution.instrumentation.parameters.InstrumentationFieldFetchParameters;
import graphql.schema.DataFetcher;
import graphql.schema.GraphQLFieldDefinition;
import graphql.schema.GraphQLObjectType;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import io.opentelemetry.sdk.trace.export.SpanExporter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Unit tests for {@link GraphQLOtelResolverInstrumentation}. Verifies the filtered span policy
 * (top-level only, allow-list, trivial-fetcher skipping), low-cardinality span naming/attributes,
 * and that skipped fields return the original fetcher with no span.
 */
public class GraphQLOtelResolverInstrumentationTest {

  /**
   * Minimal in-memory exporter so we can assert on emitted spans without the sdk-testing artifact.
   */
  private static final class CollectingSpanExporter implements SpanExporter {
    private final List<SpanData> spans = new ArrayList<>();

    @Override
    public CompletableResultCode export(Collection<SpanData> collection) {
      spans.addAll(collection);
      return CompletableResultCode.ofSuccess();
    }

    @Override
    public CompletableResultCode flush() {
      return CompletableResultCode.ofSuccess();
    }

    @Override
    public CompletableResultCode shutdown() {
      return CompletableResultCode.ofSuccess();
    }
  }

  private static final Set<String> ALLOWED_ATTRS =
      Set.of(
          "graphql.parent.type",
          "graphql.field.name",
          "graphql.resolver.name",
          "graphql.operation.type",
          "graphql.operation.name");

  private CollectingSpanExporter exporter;
  private SdkTracerProvider tracerProvider;
  private Tracer tracer;

  @BeforeMethod
  public void setUp() {
    exporter = new CollectingSpanExporter();
    tracerProvider =
        SdkTracerProvider.builder().addSpanProcessor(SimpleSpanProcessor.create(exporter)).build();
    OpenTelemetrySdk sdk = OpenTelemetrySdk.builder().setTracerProvider(tracerProvider).build();
    tracer = sdk.getTracer(GraphQLOtelResolverInstrumentation.INSTRUMENTATION_NAME);
  }

  @AfterMethod
  public void tearDown() {
    tracerProvider.close();
  }

  private static ResolverSpansConfiguration config(
      boolean topLevelOnly, boolean includeTrivial, List<String> allowedPaths) {
    ResolverSpansConfiguration c = new ResolverSpansConfiguration();
    c.setEnabled(true);
    c.setTopLevelOnly(topLevelOnly);
    c.setIncludeTrivialDataFetchers(includeTrivial);
    c.setAllowedPaths(allowedPaths);
    return c;
  }

  private static InstrumentationFieldFetchParameters params(
      String parentTypeName, String fieldName, boolean trivial) {
    GraphQLObjectType parentType = mock(GraphQLObjectType.class);
    when(parentType.getName()).thenReturn(parentTypeName);

    GraphQLFieldDefinition fieldDef = mock(GraphQLFieldDefinition.class);
    when(fieldDef.getName()).thenReturn(fieldName);

    ExecutionStepInfo parentStepInfo = mock(ExecutionStepInfo.class);
    when(parentStepInfo.getType()).thenReturn(parentType);
    ExecutionStepInfo stepInfo = mock(ExecutionStepInfo.class);
    when(stepInfo.getParent()).thenReturn(parentStepInfo);

    InstrumentationFieldFetchParameters p = mock(InstrumentationFieldFetchParameters.class);
    when(p.getField()).thenReturn(fieldDef);
    when(p.getExecutionStepInfo()).thenReturn(stepInfo);
    when(p.isTrivialDataFetcher()).thenReturn(trivial);
    // getExecutionContext() left null -> instrumentation falls back to "query"/"unnamed".
    return p;
  }

  /** Wraps a trivial sync fetcher and invokes it once, ending any span synchronously. */
  private void fetch(
      GraphQLOtelResolverInstrumentation instrumentation,
      String parentTypeName,
      String fieldName,
      boolean trivial) {
    DataFetcher<?> original = environment -> "result";
    DataFetcher<?> wrapped =
        instrumentation.instrumentDataFetcher(
            original, params(parentTypeName, fieldName, trivial), null);
    try {
      wrapped.get(null);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testTopLevelQueryFieldIsTraced() {
    GraphQLOtelResolverInstrumentation instrumentation =
        new GraphQLOtelResolverInstrumentation(config(true, false, List.of()), tracer);

    fetch(instrumentation, "Query", "searchAcrossEntities", false);

    assertEquals(exporter.spans.size(), 1);
    SpanData span = exporter.spans.get(0);
    assertEquals(span.getName(), "Query.searchAcrossEntities");
    assertEquals(span.getAttributes().get(AttributeKey.stringKey("graphql.parent.type")), "Query");
    assertEquals(
        span.getAttributes().get(AttributeKey.stringKey("graphql.field.name")),
        "searchAcrossEntities");
    assertEquals(
        span.getAttributes().get(AttributeKey.stringKey("graphql.resolver.name")),
        "Query.searchAcrossEntities");
    assertEquals(
        span.getAttributes().get(AttributeKey.stringKey("graphql.operation.type")), "query");
  }

  @Test
  public void testTopLevelMutationFieldIsTraced() {
    GraphQLOtelResolverInstrumentation instrumentation =
        new GraphQLOtelResolverInstrumentation(config(true, false, List.of()), tracer);

    fetch(instrumentation, "Mutation", "createUser", false);

    assertEquals(exporter.spans.size(), 1);
    assertEquals(exporter.spans.get(0).getName(), "Mutation.createUser");
  }

  @Test
  public void testNestedNonAllowedFieldIsNotTraced() {
    GraphQLOtelResolverInstrumentation instrumentation =
        new GraphQLOtelResolverInstrumentation(config(true, false, List.of()), tracer);

    fetch(instrumentation, "CorpUser", "username", false);

    assertEquals(exporter.spans.size(), 0, "Nested non-allowed field must not produce a span");
  }

  @Test
  public void testAllowListedNestedFieldIsTraced() {
    GraphQLOtelResolverInstrumentation instrumentation =
        new GraphQLOtelResolverInstrumentation(
            config(false, false, List.of("Entity.ownership")), tracer);

    fetch(instrumentation, "Entity", "ownership", false);

    assertEquals(exporter.spans.size(), 1);
    assertEquals(exporter.spans.get(0).getName(), "Entity.ownership");
  }

  @Test
  public void testAllowListedNestedFieldFromCsvIsTraced() {
    // allowedPaths supplied as a single comma-separated string (env-var relaxed binding).
    GraphQLOtelResolverInstrumentation instrumentation =
        new GraphQLOtelResolverInstrumentation(
            config(false, false, List.of("Entity.ownership,Entity.tags")), tracer);

    fetch(instrumentation, "Entity", "tags", false);

    assertEquals(exporter.spans.size(), 1, "CSV allowedPaths entry must split into multiple paths");
    assertEquals(exporter.spans.get(0).getName(), "Entity.tags");
  }

  @Test
  public void testTrivialDataFetcherIsNotTracedByDefault() {
    GraphQLOtelResolverInstrumentation instrumentation =
        new GraphQLOtelResolverInstrumentation(config(true, false, List.of()), tracer);

    // Top-level field but a trivial fetcher -> skipped because includeTrivialDataFetchers=false.
    fetch(instrumentation, "Query", "hello", true);

    assertEquals(
        exporter.spans.size(), 0, "Trivial fetcher must be skipped when includeTrivial=false");
  }

  @Test
  public void testTrivialDataFetcherTracedWhenEnabled() {
    GraphQLOtelResolverInstrumentation instrumentation =
        new GraphQLOtelResolverInstrumentation(config(true, true, List.of()), tracer);

    fetch(instrumentation, "Query", "hello", true);

    assertEquals(exporter.spans.size(), 1);
  }

  @Test
  public void testNoHighCardinalityAttributes() {
    GraphQLOtelResolverInstrumentation instrumentation =
        new GraphQLOtelResolverInstrumentation(config(true, false, List.of()), tracer);

    fetch(instrumentation, "Query", "searchAcrossEntities", false);

    assertEquals(exporter.spans.size(), 1);
    SpanData span = exporter.spans.get(0);
    span.getAttributes()
        .forEach(
            (key, value) ->
                assertTrue(
                    ALLOWED_ATTRS.contains(key.getKey()),
                    "Unexpected (possibly high-cardinality) attribute: " + key.getKey()));
  }

  @Test
  public void testSkippedFieldReturnsOriginalFetcherUnwrapped() {
    GraphQLOtelResolverInstrumentation instrumentation =
        new GraphQLOtelResolverInstrumentation(config(true, false, List.of()), tracer);

    DataFetcher<?> original = environment -> "result";
    DataFetcher<?> wrapped =
        instrumentation.instrumentDataFetcher(
            original, params("CorpUser", "username", false), null);

    assertSame(wrapped, original, "Skipped fields must return the original fetcher, not a wrapper");
  }

  @Test
  public void testMultipleInvocationsOfSameFieldEmitOneSpanEach() throws Exception {
    GraphQLOtelResolverInstrumentation instrumentation =
        new GraphQLOtelResolverInstrumentation(config(true, false, List.of()), tracer);

    // Same field fetched repeatedly (e.g. list of N items) -> N spans, decision memoized once.
    DataFetcher<?> original = environment -> "result";
    DataFetcher<?> wrapped =
        instrumentation.instrumentDataFetcher(original, params("Query", "users", false), null);

    for (int i = 0; i < 3; i++) {
      wrapped.get(null);
    }

    assertEquals(exporter.spans.size(), 3);
  }
}
