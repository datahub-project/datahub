package io.datahubproject.metadata.context;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.TraceFlags;
import io.opentelemetry.api.trace.TraceState;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import java.util.List;
import java.util.Map;
import org.testng.annotations.Test;

public class RequestStatsTest {

  private static final String TRACE_ID = "0af7651916cd43dd8448eb211c80319c";
  private static final String SPAN_ID = "b7ad6b7169203331";

  private static Span validSpan() {
    Span span = mock(Span.class);
    when(span.getSpanContext())
        .thenReturn(
            SpanContext.create(
                TRACE_ID, SPAN_ID, TraceFlags.getSampled(), TraceState.getDefault()));
    return span;
  }

  @Test
  public void currentIsEmptyOutsideScopeAndPresentInside() {
    assertFalse(RequestStats.current().isPresent());
    RequestStats stats = new RequestStats(false);
    try (Scope ignored = Context.current().with(RequestStats.CONTEXT_KEY, stats).makeCurrent()) {
      assertTrue(RequestStats.current().isPresent());
      assertEquals(RequestStats.current().get(), stats);
    }
    assertFalse(RequestStats.current().isPresent());
  }

  @Test
  public void graphqlVariablesYieldSizeArguments() {
    RequestStats stats = new RequestStats(false);
    stats.recordGraphqlVariables(
        Map.of(
            "input", Map.of("query", "*", "start", 20, "count", 50, "types", List.of("DATASET"))));
    assertEquals(stats.getRequestCount(), Long.valueOf(50));
    assertEquals(stats.getRequestStart(), Long.valueOf(20));

    RequestStats limit = new RequestStats(false);
    limit.recordGraphqlVariables(Map.of("input", Map.of("limit", 7)));
    assertEquals(limit.getRequestCount(), Long.valueOf(7));
    assertNull(limit.getRequestStart());
  }

  @Test
  public void opaqueIdRequiresFlagAndValidTrace() {
    RequestStats disabled = new RequestStats(false);
    assertFalse(disabled.opaqueId().isPresent());

    RequestStats enabled = new RequestStats(true);
    assertFalse(enabled.opaqueId().isPresent(), "no valid span current");

    enabled.attach(null, "urn:li:corpuser:jdoe", "searchAcrossEntities");
    // A real (propagated) span: Mockito mocks return null from storeInContext.
    Span span =
        Span.wrap(
            SpanContext.create(
                TRACE_ID, SPAN_ID, TraceFlags.getSampled(), TraceState.getDefault()));
    try (Scope ignored = Context.current().with(span).makeCurrent()) {
      assertEquals(
          enabled.opaqueId().get(),
          "trace=" + TRACE_ID + "|actor=urn:li:corpuser:jdoe|req=searchAcrossEntities");
    }
  }

  @Test
  public void finishWritesStoreTimingsToPinnedSpan() {
    RequestStats stats = new RequestStats(false);
    stats.recordSearch(2_000_000L);
    stats.recordSearch(3_000_000L);
    stats.recordDb(500_000L);
    Span pinned = validSpan();
    Span fallback = validSpan();
    stats.attach(pinned, "urn:li:corpuser:jdoe", "getDataset");
    stats.finish(fallback);
    verify(pinned).setAttribute(eq(RequestStats.ES_CALLS), eq(2L));
    verify(pinned).setAttribute(eq(RequestStats.ES_TIME_MS), eq(5.0d));
    verify(pinned).setAttribute(eq(RequestStats.PG_CONN_CALLS), eq(1L));
    verify(pinned).setAttribute(eq(RequestStats.PG_CONN_MS), eq(0.5d));
    verify(fallback, never()).setAttribute(eq(RequestStats.ES_CALLS), eq(2L));
  }

  @Test
  public void finishUsesFallbackWhenNothingPinned() {
    RequestStats stats = new RequestStats(false);
    stats.recordSearch(1_000_000L);
    Span fallback = validSpan();
    stats.finish(fallback);
    verify(fallback).setAttribute(eq(RequestStats.ES_CALLS), eq(1L));
  }
}
