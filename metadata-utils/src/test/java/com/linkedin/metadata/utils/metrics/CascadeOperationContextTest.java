package com.linkedin.metadata.utils.metrics;

import static org.testng.Assert.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.mxe.SystemMetadata;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.slf4j.MDC;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class CascadeOperationContextTest {

  private SimpleMeterRegistry meterRegistry;
  private MetricUtils metricUtils;

  @BeforeMethod
  public void setup() {
    MDC.clear();
    // Use a dedicated SimpleMeterRegistry wrapped in a CompositeMeterRegistry.
    // MetricUtils has a static counter cache, so we use a fresh MetricUtils instance with
    // a unique registry per test to avoid cross-test cache interference.
    meterRegistry = new SimpleMeterRegistry();
    metricUtils = MetricUtils.builder().registry(meterRegistry).build();
  }

  @AfterMethod
  public void cleanup() {
    MDC.clear();
  }

  @Test
  public void testMDCSetOnBeginAndClearedOnClose() {
    Urn triggerUrn = UrnUtils.getUrn("urn:li:tag:testTag");

    try (CascadeOperationContext ctx =
        CascadeOperationContext.begin(metricUtils, "deleteReferencesTo", triggerUrn, 100)) {
      assertEquals(MDC.get("cascade.operation.type"), "deleteReferencesTo");
      assertEquals(MDC.get("cascade.trigger.urn"), "urn:li:tag:testTag");
      assertNotNull(MDC.get("cascade.operation.id"));
    }

    assertNull(MDC.get("cascade.operation.id"));
    assertNull(MDC.get("cascade.trigger.urn"));
    assertNull(MDC.get("cascade.operation.type"));
  }

  @Test
  public void testCloseWithMetricsDoesNotThrow() {
    Urn triggerUrn = UrnUtils.getUrn("urn:li:tag:testTag");

    // Verify that close() completes without exception when recording metrics
    try (CascadeOperationContext ctx =
        CascadeOperationContext.begin(metricUtils, "deleteReferencesTo", triggerUrn, 50)) {
      ctx.recordEntityProcessed();
      ctx.recordEntityProcessed();
      ctx.recordEntityProcessed();
    }
    // If we get here, close() succeeded — metrics were emitted without error
    assertNull(MDC.get("cascade.operation.id"), "MDC should be cleared after close");
  }

  @Test
  public void testCloseWithErrorsDoesNotThrow() {
    Urn triggerUrn = UrnUtils.getUrn("urn:li:tag:testTag");

    // Verify that close() with errors completes without exception
    try (CascadeOperationContext ctx =
        CascadeOperationContext.begin(metricUtils, "deleteReferencesTo", triggerUrn, 10)) {
      ctx.recordEntityProcessed();
      ctx.recordError("clone_failed");
    }
    // If we get here, close() succeeded with error metrics
    assertNull(MDC.get("cascade.operation.id"), "MDC should be cleared after close");
  }

  @Test
  public void testNullMetricUtilsHandledGracefully() {
    Urn triggerUrn = UrnUtils.getUrn("urn:li:tag:testTag");

    // Should not throw
    try (CascadeOperationContext ctx =
        CascadeOperationContext.begin(null, "deleteReferencesTo", triggerUrn, 10)) {
      ctx.recordEntityProcessed();
      ctx.recordError("test_error");
    }

    // MDC should still be cleared
    assertNull(MDC.get("cascade.operation.id"));
  }

  @Test
  public void testAttachToSystemMetadataWithNullProperties() {
    Urn triggerUrn = UrnUtils.getUrn("urn:li:tag:testTag");
    SystemMetadata systemMetadata = new SystemMetadata();
    assertNull(systemMetadata.getProperties());

    try (CascadeOperationContext ctx =
        CascadeOperationContext.begin(metricUtils, "deleteReferencesTo", triggerUrn, 1)) {
      ctx.attachToSystemMetadata(systemMetadata);

      assertNotNull(systemMetadata.getProperties());
      assertEquals(
          systemMetadata
              .getProperties()
              .get(CascadeOperationContext.SYSTEM_METADATA_CASCADE_ID_KEY),
          ctx.getOperationId());
    }
  }

  @Test
  public void testAttachToSystemMetadataWithExistingProperties() {
    Urn triggerUrn = UrnUtils.getUrn("urn:li:tag:testTag");
    SystemMetadata systemMetadata = new SystemMetadata();
    systemMetadata.setProperties(new com.linkedin.data.template.StringMap());
    systemMetadata.getProperties().put("existingKey", "existingValue");

    try (CascadeOperationContext ctx =
        CascadeOperationContext.begin(metricUtils, "deleteReferencesTo", triggerUrn, 1)) {
      ctx.attachToSystemMetadata(systemMetadata);

      assertEquals(systemMetadata.getProperties().get("existingKey"), "existingValue");
      assertEquals(
          systemMetadata
              .getProperties()
              .get(CascadeOperationContext.SYSTEM_METADATA_CASCADE_ID_KEY),
          ctx.getOperationId());
    }
  }

  @Test
  public void testAttachToNullSystemMetadataDoesNotThrow() {
    Urn triggerUrn = UrnUtils.getUrn("urn:li:tag:testTag");

    try (CascadeOperationContext ctx =
        CascadeOperationContext.begin(metricUtils, "deleteReferencesTo", triggerUrn, 1)) {
      // Should not throw
      ctx.attachToSystemMetadata(null);
    }
  }

  @Test
  public void testOperationIdIsUnique() {
    Urn triggerUrn = UrnUtils.getUrn("urn:li:tag:testTag");

    String id1;
    String id2;
    try (CascadeOperationContext ctx1 =
        CascadeOperationContext.begin(metricUtils, "deleteReferencesTo", triggerUrn, 1)) {
      id1 = ctx1.getOperationId();
    }
    try (CascadeOperationContext ctx2 =
        CascadeOperationContext.begin(metricUtils, "deleteReferencesTo", triggerUrn, 1)) {
      id2 = ctx2.getOperationId();
    }

    assertNotEquals(id1, id2);
  }

  @Test
  public void testNestedCascadeRestoresMDC() {
    Urn outerUrn = UrnUtils.getUrn("urn:li:tag:outerTag");
    Urn innerUrn = UrnUtils.getUrn("urn:li:structuredProperty:innerProp");

    try (CascadeOperationContext outer =
        CascadeOperationContext.begin(metricUtils, "deleteReferencesTo", outerUrn, 100)) {
      String outerOpId = MDC.get("cascade.operation.id");
      assertEquals(MDC.get("cascade.operation.type"), "deleteReferencesTo");
      assertNotNull(outerOpId);

      // Inner cascade overwrites MDC
      try (CascadeOperationContext inner =
          CascadeOperationContext.begin(metricUtils, "propertyDefinitionDelete", innerUrn, 50)) {
        assertEquals(MDC.get("cascade.operation.type"), "propertyDefinitionDelete");
        assertNotEquals(MDC.get("cascade.operation.id"), outerOpId);
      }

      // After inner closes, outer MDC is restored
      assertEquals(MDC.get("cascade.operation.id"), outerOpId);
      assertEquals(MDC.get("cascade.operation.type"), "deleteReferencesTo");
      assertEquals(MDC.get("cascade.trigger.urn"), "urn:li:tag:outerTag");
    }

    // After outer closes, MDC is fully cleared
    assertNull(MDC.get("cascade.operation.id"));
    assertNull(MDC.get("cascade.operation.type"));
    assertNull(MDC.get("cascade.trigger.urn"));
  }

  @Test
  public void testBeginWithoutMDCDoesNotTouchMDC() {
    Urn triggerUrn = UrnUtils.getUrn("urn:li:tag:testTag");

    // Pre-populate MDC to verify it is not touched
    MDC.put("cascade.operation.id", "pre-existing-id");
    MDC.put("cascade.operation.type", "pre-existing-type");

    try (CascadeOperationContext ctx =
        CascadeOperationContext.beginWithoutMDC(
            metricUtils, "propertyDefinitionDelete", triggerUrn, -1)) {
      // MDC should still have the pre-existing values, not the new cascade values
      assertEquals(MDC.get("cascade.operation.id"), "pre-existing-id");
      assertEquals(MDC.get("cascade.operation.type"), "pre-existing-type");

      // Metrics still work
      ctx.recordEntityProcessed();
      ctx.recordEntityProcessed();
    }

    // After close, pre-existing MDC values should be untouched
    assertEquals(MDC.get("cascade.operation.id"), "pre-existing-id");
    assertEquals(MDC.get("cascade.operation.type"), "pre-existing-type");
  }

  @Test
  public void testBeginWithoutMDCCloseDoesNotThrow() {
    Urn triggerUrn = UrnUtils.getUrn("urn:li:tag:testTag");

    // Should not throw even with null metricUtils
    try (CascadeOperationContext ctx =
        CascadeOperationContext.beginWithoutMDC(null, "testOp", triggerUrn, 5)) {
      ctx.recordEntityProcessed();
      ctx.recordError("test_error");
    }
    // No assertions needed — if we get here, close() succeeded
  }

  @Test
  public void testMetricValuesEmittedCorrectly() {
    Urn triggerUrn = UrnUtils.getUrn("urn:li:tag:testTag");
    // Use a unique operation type to avoid MetricUtils static cache interference across tests
    String opType = "metricTest_noErrors";

    try (CascadeOperationContext ctx =
        CascadeOperationContext.begin(metricUtils, opType, triggerUrn, 100)) {
      ctx.recordEntityProcessed();
      ctx.recordEntityProcessed();
      ctx.recordEntityProcessed();
    }

    // Verify entities_processed counter
    Counter entitiesCounter =
        meterRegistry
            .find("datahub.cascade.entities_processed")
            .tag("operation_type", opType)
            .counter();
    assertNotNull(entitiesCounter, "entities_processed counter should exist");
    assertEquals(entitiesCounter.count(), 3.0);

    // Verify duration timer was recorded
    Timer durationTimer =
        meterRegistry.find("datahub.cascade.duration").tag("operation_type", opType).timer();
    assertNotNull(durationTimer, "duration timer should exist");
    assertEquals(durationTimer.count(), 1L);
    assertTrue(durationTimer.totalTime(java.util.concurrent.TimeUnit.NANOSECONDS) > 0);

    // Verify no error counter (no errors recorded)
    Counter errorCounter =
        meterRegistry.find("datahub.cascade.errors").tag("operation_type", opType).counter();
    assertNull(errorCounter, "error counter should not exist when no errors");
  }

  @Test
  public void testMetricValuesWithErrors() {
    Urn triggerUrn = UrnUtils.getUrn("urn:li:structuredProperty:testProp");
    // Use a unique operation type to avoid MetricUtils static cache interference across tests
    String opType = "metricTest_withErrors";

    try (CascadeOperationContext ctx =
        CascadeOperationContext.begin(metricUtils, opType, triggerUrn, 10)) {
      ctx.recordEntityProcessed();
      ctx.recordEntityProcessed();
      ctx.recordError("clone_failed");
      ctx.recordError("mcp_processor_failed");
    }

    // Verify entities_processed counter
    Counter entitiesCounter =
        meterRegistry
            .find("datahub.cascade.entities_processed")
            .tag("operation_type", opType)
            .counter();
    assertNotNull(entitiesCounter);
    assertEquals(entitiesCounter.count(), 2.0);

    // Verify duration timer has completed_with_errors status
    Timer durationTimer =
        meterRegistry
            .find("datahub.cascade.duration")
            .tag("status", "completed_with_errors")
            .timer();
    assertNotNull(durationTimer, "duration timer should have completed_with_errors status");

    // Verify error counter — total count is 2 (two recordError calls)
    Counter errorCounter =
        meterRegistry.find("datahub.cascade.errors").tag("operation_type", opType).counter();
    assertNotNull(errorCounter, "error counter should exist");
    assertEquals(errorCounter.count(), 2.0);
  }

  @Test
  public void testCloseIsIdempotent() {
    Urn triggerUrn = UrnUtils.getUrn("urn:li:tag:testTag");
    String opType = "idempotentTest";

    CascadeOperationContext ctx =
        CascadeOperationContext.begin(metricUtils, opType, triggerUrn, 10);
    ctx.recordEntityProcessed();
    ctx.recordEntityProcessed();

    // First close — should emit metrics
    ctx.close();

    Counter entitiesCounter =
        meterRegistry
            .find("datahub.cascade.entities_processed")
            .tag("operation_type", opType)
            .counter();
    assertNotNull(entitiesCounter);
    assertEquals(entitiesCounter.count(), 2.0);

    Timer durationTimer =
        meterRegistry.find("datahub.cascade.duration").tag("operation_type", opType).timer();
    assertNotNull(durationTimer);
    assertEquals(durationTimer.count(), 1L);

    // Second close — should be a no-op, metrics unchanged
    ctx.close();

    assertEquals(
        entitiesCounter.count(), 2.0, "entities counter should not double after second close");
    assertEquals(durationTimer.count(), 1L, "duration timer should not record a second sample");

    // MDC should still be cleaned up
    assertNull(MDC.get("cascade.operation.id"));
  }
}
