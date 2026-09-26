package com.linkedin.metadata.search.elasticsearch.client.shim.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.search.utils.ESUtils;
import io.datahubproject.metadata.context.RequestStats;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.TraceFlags;
import io.opentelemetry.api.trace.TraceState;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import org.opensearch.client.RequestOptions;
import org.testng.annotations.Test;

public class ShimTelemetryTest {

  @Test
  public void withOpaqueIdIsPassThroughWithoutStatsOrFlagOrTrace() {
    RequestOptions options = RequestOptions.DEFAULT;
    assertSame(ShimTelemetry.withOpaqueId(options, null), options);
    assertSame(ShimTelemetry.withOpaqueId(options, new RequestStats(false)), options);
    // flag on but no valid trace in scope
    assertSame(ShimTelemetry.withOpaqueId(options, new RequestStats(true)), options);
  }

  @Test
  public void withOpaqueIdAddsHeaderInsideAValidTrace() {
    RequestStats stats = new RequestStats(true);
    stats.attach(null, "urn:li:corpuser:jdoe", "searchAcrossEntities");
    Span span =
        Span.wrap(
            SpanContext.create(
                "0af7651916cd43dd8448eb211c80319c",
                "b7ad6b7169203331",
                TraceFlags.getSampled(),
                TraceState.getDefault()));
    try (Scope ignored = Context.current().with(span).makeCurrent()) {
      RequestOptions out = ShimTelemetry.withOpaqueId(RequestOptions.DEFAULT, stats);
      String id =
          out.getHeaders().stream()
              .filter(h -> h.getName().equals(ESUtils.OPAQUE_ID_HEADER))
              .map(h -> h.getValue())
              .findFirst()
              .orElse(null);
      assertEquals(
          id,
          "trace=0af7651916cd43dd8448eb211c80319c|actor=urn:li:corpuser:jdoe|req=searchAcrossEntities|n=1");
    }
  }

  @Test
  public void recordSearchAccumulatesOnlyWithStats() {
    ShimTelemetry.recordSearch(null, System.nanoTime());
    RequestStats stats = new RequestStats(false);
    ShimTelemetry.recordSearch(stats, System.nanoTime() - 1_000_000L);
    assertEquals(stats.getEsCalls(), 1L);
    assertTrue(stats.getEsNanos() >= 1_000_000L);
    assertNull(stats.getRequestCount());
  }
}
