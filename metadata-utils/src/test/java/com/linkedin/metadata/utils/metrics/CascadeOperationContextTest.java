package com.linkedin.metadata.utils.metrics;

import static org.testng.Assert.*;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.mxe.SystemMetadata;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.slf4j.LoggerFactory;
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
  public void testOutcomeIsNotAnError() {
    Urn triggerUrn = UrnUtils.getUrn("urn:li:form:testForm");
    String opType = "metricTest_outcome";

    try (CascadeOperationContext ctx =
        CascadeOperationContext.begin(metricUtils, opType, triggerUrn, 10)) {
      ctx.recordOutcome("referencing_entity_kept");
      ctx.recordOutcome("referencing_entity_kept");
    }

    Counter errorCounter =
        meterRegistry.find("datahub.cascade.errors").tag("operation_type", opType).counter();
    assertNull(errorCounter, "a kept entity is not an error");

    Timer durationTimer =
        meterRegistry
            .find("datahub.cascade.duration")
            .tag("operation_type", opType)
            .tag("status", "completed")
            .timer();
    assertNotNull(durationTimer, "outcomes alone complete without errors");

    Counter outcomeCounter =
        meterRegistry
            .find("datahub.cascade.outcomes")
            .tag("operation_type", opType)
            .tag("outcome", "referencing_entity_kept")
            .counter();
    assertNotNull(outcomeCounter, "kept entities are counted on their own");
    assertEquals(outcomeCounter.count(), 2.0);
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

  @Test
  public void testCustomMetricPrefix() {
    Urn triggerUrn = UrnUtils.getUrn("urn:li:tag:testTag");
    String opType = "customPrefixTest";

    try (CascadeOperationContext ctx =
        CascadeOperationContext.begin(metricUtils, opType, triggerUrn, 10, "datahub.lineage")) {
      ctx.recordEntityProcessed();
      ctx.recordEntityProcessed();
    }

    // Verify metrics are emitted under the custom prefix
    Counter entitiesCounter =
        meterRegistry
            .find("datahub.lineage.entities_processed")
            .tag("operation_type", opType)
            .counter();
    assertNotNull(entitiesCounter, "entities_processed counter should exist under custom prefix");
    assertEquals(entitiesCounter.count(), 2.0);

    Timer durationTimer =
        meterRegistry.find("datahub.lineage.duration").tag("operation_type", opType).timer();
    assertNotNull(durationTimer, "duration timer should exist under custom prefix");

    // Verify the default prefix was NOT used for this operation type
    Counter defaultCounter =
        meterRegistry
            .find("datahub.cascade.entities_processed")
            .tag("operation_type", opType)
            .counter();
    assertNull(
        defaultCounter, "default prefix counter should not exist for custom prefix operation");
  }

  @Test
  public void testTagsReachDurationTimerAndLastValueWins() {
    Urn triggerUrn = UrnUtils.getUrn("urn:li:tag:testTag");
    String opType = "taggedTest";

    try (CascadeOperationContext ctx =
        CascadeOperationContext.begin(metricUtils, opType, triggerUrn, -1, "datahub.lineage")) {
      ctx.tag("kind", "impact").tag("cache", "skip");
      ctx.tag("cache", "miss");
    }

    Timer timer =
        meterRegistry
            .find("datahub.lineage.duration")
            .tags("operation_type", opType, "kind", "impact", "cache", "miss")
            .timer();
    assertNotNull(timer, "duration timer should carry the tags set during the operation");
    assertEquals(timer.count(), 1L);
    assertNull(
        meterRegistry
            .find("datahub.lineage.duration")
            .tags("operation_type", opType, "cache", "skip")
            .timer(),
        "an overwritten tag value should not be emitted");

    Counter entitiesCounter =
        meterRegistry
            .find("datahub.lineage.entities_processed")
            .tag("operation_type", opType)
            .counter();
    assertNotNull(entitiesCounter);
    assertNull(entitiesCounter.getId().getTag("kind"), "tags apply to the duration timer only");
  }

  @Test
  public void testLogLineCarriesMaxPerHopAndTagsOnlyWhenRecorded() {
    Urn triggerUrn = UrnUtils.getUrn("urn:li:tag:testTag");
    Logger logger = (Logger) LoggerFactory.getLogger(CascadeOperationContext.class);
    ListAppender<ILoggingEvent> appender = new ListAppender<>();
    appender.start();
    logger.addAppender(appender);
    try {
      // The error line is logged whatever the duration, so no need to wait for the slow threshold
      try (CascadeOperationContext ctx =
          CascadeOperationContext.begin(
              metricUtils, "walkTest", triggerUrn, -1, "datahub.lineage.graph_walk")) {
        ctx.tag("kind", "impact");
        ctx.recordHop(1);
        ctx.recordHop(340);
        ctx.recordHop(95);
        ctx.recordError("timeout");
      }
      try (CascadeOperationContext ctx =
          CascadeOperationContext.begin(metricUtils, "deleteTest", triggerUrn, -1)) {
        ctx.recordError("failure");
      }

      List<String> lines =
          appender.list.stream()
              .map(ILoggingEvent::getFormattedMessage)
              .filter(l -> l.startsWith("Cascade completed"))
              .toList();
      String walkLine =
          lines.stream().filter(l -> l.contains("type=walkTest")).findFirst().orElseThrow();
      assertTrue(walkLine.contains("hops=3"), walkLine);
      assertTrue(walkLine.endsWith("ms, maxPerHop=340, kind=impact"), walkLine);

      String deleteLine =
          lines.stream().filter(l -> l.contains("type=deleteTest")).findFirst().orElseThrow();
      assertTrue(deleteLine.endsWith("ms"), "no hops and no tags keep the old line: " + deleteLine);
    } finally {
      logger.detachAppender(appender);
    }
  }

  @Test
  public void testPhasesReachLogLineButNotTimer() {
    Urn triggerUrn = UrnUtils.getUrn("urn:li:tag:testTag");
    Logger logger = (Logger) LoggerFactory.getLogger(CascadeOperationContext.class);
    ListAppender<ILoggingEvent> appender = new ListAppender<>();
    appender.start();
    logger.addAppender(appender);
    try {
      try (CascadeOperationContext ctx =
          CascadeOperationContext.begin(
              metricUtils, "phaseTest", triggerUrn, -1, "datahub.lineage")) {
        ctx.tag("kind", "impact");
        ctx.recordPhase("walk", TimeUnit.MILLISECONDS.toNanos(2));
        ctx.recordPhase("search", TimeUnit.MILLISECONDS.toNanos(7));
        // Repeated phases add up
        ctx.recordPhase("walk", TimeUnit.MILLISECONDS.toNanos(3));
        ctx.recordError("timeout");
      }

      String line =
          appender.list.stream()
              .map(ILoggingEvent::getFormattedMessage)
              .filter(l -> l.startsWith("Cascade completed") && l.contains("type=phaseTest"))
              .findFirst()
              .orElseThrow();
      assertTrue(line.endsWith("kind=impact, walkMs=5, searchMs=7"), line);

      Timer timer =
          meterRegistry.find("datahub.lineage.duration").tag("operation_type", "phaseTest").timer();
      assertNotNull(timer);
      assertNull(timer.getId().getTag("walkMs"));
      assertNull(timer.getId().getTag("walk"));
    } finally {
      logger.detachAppender(appender);
    }
  }

  @Test
  public void testFastCompletionLogsAtInfoOnlyWhenOptedIn() {
    Urn triggerUrn = UrnUtils.getUrn("urn:li:tag:testTag");
    Logger logger = (Logger) LoggerFactory.getLogger(CascadeOperationContext.class);
    ListAppender<ILoggingEvent> appender = new ListAppender<>();
    appender.start();
    logger.addAppender(appender);
    try {
      try (CascadeOperationContext ctx =
          CascadeOperationContext.begin(
                  metricUtils, "fastWalk", triggerUrn, -1, "datahub.lineage.graph_walk")
              .logCompletionAtInfo()) {
        ctx.recordHop(2);
      }
      try (CascadeOperationContext ctx =
          CascadeOperationContext.begin(metricUtils, "fastDelete", triggerUrn, -1)) {
        ctx.recordEntityProcessed();
      }

      List<String> infoLines =
          appender.list.stream()
              .filter(e -> e.getLevel() == Level.INFO)
              .map(ILoggingEvent::getFormattedMessage)
              .toList();
      assertTrue(
          infoLines.stream().anyMatch(l -> l.startsWith("Cascade completed: type=fastWalk")),
          infoLines.toString());
      assertTrue(
          infoLines.stream().noneMatch(l -> l.contains("type=fastDelete")), infoLines.toString());
    } finally {
      logger.detachAppender(appender);
    }
  }

  @Test
  public void testRecordHop() {
    Urn triggerUrn = UrnUtils.getUrn("urn:li:tag:testTag");
    String opType = "hopTrackingTest";
    String prefix = "datahub.lineage.graph_walk";

    try (CascadeOperationContext ctx =
        CascadeOperationContext.begin(metricUtils, opType, triggerUrn, -1, prefix)) {
      ctx.recordHop(1);
      ctx.recordEntitiesProcessed(100);
      ctx.recordHop(1);
      ctx.recordEntitiesProcessed(50);
      ctx.recordHop(1);
    }

    Counter hopsCounter =
        meterRegistry.find(prefix + ".hops").tag("operation_type", opType).counter();
    assertNotNull(hopsCounter, "hops counter should exist");
    assertEquals(hopsCounter.count(), 3.0);

    Counter entitiesCounter =
        meterRegistry.find(prefix + ".entities_processed").tag("operation_type", opType).counter();
    assertNotNull(entitiesCounter, "entities_processed counter should exist");
    assertEquals(entitiesCounter.count(), 150.0);
  }

  @Test
  public void testHopsNotEmittedWhenZero() {
    Urn triggerUrn = UrnUtils.getUrn("urn:li:tag:testTag");
    String opType = "noHopsTest";

    try (CascadeOperationContext ctx =
        CascadeOperationContext.begin(metricUtils, opType, triggerUrn, 5)) {
      ctx.recordEntityProcessed();
      // No recordHop() calls
    }

    Counter hopsCounter =
        meterRegistry.find("datahub.cascade.hops").tag("operation_type", opType).counter();
    assertNull(hopsCounter, "hops counter should not exist when no hops were recorded");
  }

  @Test
  public void testNullTriggerUrn() {
    String opType = "nullTriggerTest";
    String prefix = "datahub.filter_rewrite";

    try (CascadeOperationContext ctx =
        CascadeOperationContext.begin(metricUtils, opType, null, 10, prefix)) {
      ctx.recordEntityProcessed();
      ctx.recordHop(1);
    }

    Counter entitiesCounter =
        meterRegistry
            .find(prefix + ".entities_processed")
            .tag("operation_type", opType)
            .tag("trigger_urn_type", "unknown")
            .counter();
    assertNotNull(
        entitiesCounter, "entities_processed counter should exist with trigger_urn_type=unknown");
    assertEquals(entitiesCounter.count(), 1.0);

    Counter hopsCounter =
        meterRegistry
            .find(prefix + ".hops")
            .tag("operation_type", opType)
            .tag("trigger_urn_type", "unknown")
            .counter();
    assertNotNull(hopsCounter, "hops counter should exist with trigger_urn_type=unknown");
    assertEquals(hopsCounter.count(), 1.0);

    // The duration timer leaves the entity type to the counters
    Timer timer = meterRegistry.find(prefix + ".duration").tag("operation_type", opType).timer();
    assertNotNull(timer);
    assertNull(timer.getId().getTag("trigger_urn_type"));
  }
}
