package com.linkedin.metadata.utils.elasticsearch;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Tracer;
import org.testng.annotations.Test;

public class BulkTelemetryConfigTest {

  @Test
  public void disabledIsAllOff() {
    BulkTelemetryConfig off = BulkTelemetryConfig.DISABLED;
    assertNull(off.getTracer());
    assertFalse(off.isBatchSpans());
    assertFalse(off.isOpaqueId());
    assertNull(off.getServiceName());
    assertFalse(off.spansEnabled());
    assertFalse(off.isEnabled());
    assertEquals(BulkTelemetryConfig.of(null, false, false, null), off);
  }

  @Test
  public void spansNeedBothTheFlagAndATracer() {
    Tracer tracer = OpenTelemetry.noop().getTracer("test");
    assertFalse(BulkTelemetryConfig.of(null, true, false, "svc").spansEnabled());
    assertFalse(BulkTelemetryConfig.of(null, true, false, "svc").isEnabled());
    assertFalse(BulkTelemetryConfig.of(tracer, false, false, "svc").spansEnabled());
    assertTrue(BulkTelemetryConfig.of(tracer, true, false, "svc").spansEnabled());
    assertTrue(BulkTelemetryConfig.of(tracer, true, false, "svc").isEnabled());
  }

  @Test
  public void headerAloneIsEnabled() {
    BulkTelemetryConfig headerOnly = BulkTelemetryConfig.of(null, false, true, "gms");
    assertTrue(headerOnly.isEnabled());
    assertFalse(headerOnly.spansEnabled());
    assertEquals(headerOnly.getServiceName(), "gms");
  }

  @Test
  public void isAValueWithABuilder() {
    Tracer tracer = OpenTelemetry.noop().getTracer("test");
    BulkTelemetryConfig a = BulkTelemetryConfig.of(tracer, true, true, "gms");
    BulkTelemetryConfig b =
        BulkTelemetryConfig.builder()
            .tracer(tracer)
            .batchSpans(true)
            .opaqueId(true)
            .serviceName("gms")
            .build();
    assertEquals(a, b);
    assertEquals(a.hashCode(), b.hashCode());
    assertNotEquals(a, a.toBuilder().opaqueId(false).build());
    assertSame(a.getTracer(), tracer);
    assertTrue(a.toString().contains("gms"));
  }
}
