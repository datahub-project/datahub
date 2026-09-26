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
          "trace=" + TRACE_ID + "|actor=urn:li:corpuser:jdoe|req=searchAcrossEntities|n=1");
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

  @Test
  public void timeoutBackendPidsAndSummary() {
    RequestStats stats = new RequestStats(false);
    assertEquals(stats.getTimeoutAtNanos(), 0L);
    stats.recordSearch(2_000_000L);
    stats.recordSearch(-5L); // clamped to zero, still counted
    stats.recordDb(1_500_000L);
    stats.recordDbBackendPid(42);
    stats.recordDbBackendPid(7);
    stats.recordDbBackendPid(42);
    stats.recordDbBackendPid(-1); // unknown pid is ignored
    assertEquals(stats.getBackendPids(), java.util.Set.of(42L, 7L));
    assertEquals(stats.summary(), "es=2 calls/2.0 ms, pg=1 borrows/1.5 ms");

    stats.markTimeout();
    long first = stats.getTimeoutAtNanos();
    assertTrue(first > 0L);
    stats.markTimeout();
    assertEquals(stats.getTimeoutAtNanos(), first, "first timeout mark wins");

    stats.attach(null, "urn:li:corpuser:jdoe", "getDataset");
    assertEquals(stats.getActorUrn(), "urn:li:corpuser:jdoe");
    assertEquals(stats.getRequestId(), "getDataset");

    Span span = validSpan();
    stats.finish(span);
    verify(span).setAttribute(eq(RequestStats.REQUEST_TIMEOUT), eq(true));
    verify(span).setAttribute(eq(RequestStats.PG_BACKEND_PIDS), eq(List.of(7L, 42L)));
  }

  @Test
  public void backendPidsAreCapped() {
    RequestStats stats = new RequestStats(false);
    for (int i = 1; i <= 100; i++) {
      stats.recordDbBackendPid(i);
    }
    assertEquals(stats.getBackendPids().size(), 32);
  }

  @Test
  public void graphqlVariablesHandleNullEmptyAndNestedCollections() {
    RequestStats stats = new RequestStats(false);
    stats.recordGraphqlVariables(null);
    stats.recordGraphqlVariables(Map.of());
    assertNull(stats.getRequestCount());
    // count nested inside a list of maps, non-numeric count ignored, nested lists walked
    stats.recordGraphqlVariables(
        Map.of(
            "inputs",
            List.of(List.of("x"), Map.of("count", "notanumber"), Map.of("count", 12, "start", 3))));
    assertEquals(stats.getRequestCount(), Long.valueOf(12));
    assertEquals(stats.getRequestStart(), Long.valueOf(3));
    // first occurrence wins
    stats.recordGraphqlVariables(Map.of("count", 99, "start", 99));
    assertEquals(stats.getRequestCount(), Long.valueOf(12));
  }

  @Test
  public void attachIgnoresInvalidSpanAndNulls() {
    RequestStats stats = new RequestStats(false);
    Span invalid = mock(Span.class);
    when(invalid.getSpanContext()).thenReturn(SpanContext.getInvalid());
    stats.attach(invalid, null, null);
    assertNull(stats.getActorUrn());
    Span fallback = validSpan();
    stats.finish(fallback); // invalid span was not pinned, fallback is used
    verify(fallback).setAttribute(eq(RequestStats.ES_CALLS), eq(0L));
    stats.finish(null); // nothing pinned, no fallback: no-op
  }

  @Test
  public void finishWritesRequestSizeArgumentsAndAccessorsAgree() {
    RequestStats stats = new RequestStats(false);
    stats.recordGraphqlVariables(Map.of("input", Map.of("count", 25, "start", 50)));
    stats.recordSearch(3_000_000L);
    stats.recordDb(4_000_000L);
    assertEquals(stats.getEsCalls(), 1L);
    assertEquals(stats.getEsNanos(), 3_000_000L);
    assertEquals(stats.getDbCalls(), 1L);
    assertEquals(stats.getDbNanos(), 4_000_000L);
    Span span = validSpan();
    stats.finish(span);
    verify(span).setAttribute(eq(RequestStats.REQUEST_COUNT), eq(25L));
    verify(span).setAttribute(eq(RequestStats.REQUEST_START), eq(50L));
  }
}
