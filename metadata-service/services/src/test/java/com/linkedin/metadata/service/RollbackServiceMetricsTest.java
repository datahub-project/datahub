package com.linkedin.metadata.service;

import static io.datahubproject.test.search.SearchTestUtils.TEST_SYSTEM_METADATA_SERVICE_CONFIG;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.config.shared.ResultsLimitConfig;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.RollbackResult;
import com.linkedin.metadata.entity.RollbackRunResult;
import com.linkedin.metadata.run.AspectRowSummary;
import com.linkedin.metadata.systemmetadata.SystemMetadataService;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import com.linkedin.timeseries.DeleteAspectValuesResult;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.SystemTelemetryContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.opentelemetry.api.OpenTelemetry;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Behavioural coverage for ingestion-rollback Micrometer wiring (see vault ADR 2026-08-25 classify
 * and instrument long-running GMS operations).
 */
public class RollbackServiceMetricsTest {

  private static final String TEST_RUN_ID = "metrics-rollback-run";
  private static final String TEST_URN_1 =
      "urn:li:dataset:(urn:li:dataPlatform:hive,metrics-rollback-1,PROD)";
  private static final String TEST_URN_2 =
      "urn:li:dataset:(urn:li:dataPlatform:hive,metrics-rollback-2,PROD)";
  private static final int MAX_SEARCH_RESULTS = 5;

  private EntityService<?> mockEntityService;
  private SystemMetadataService mockSystemMetadataService;
  private TimeseriesAspectService mockTimeseriesAspectService;
  private RollbackService rollbackService;
  private SimpleMeterRegistry meterRegistry;
  private OperationContext opContext;

  @BeforeMethod
  public void setUp() {
    mockEntityService = mock(EntityService.class);
    mockSystemMetadataService = mock(SystemMetadataService.class);
    mockTimeseriesAspectService = mock(TimeseriesAspectService.class);
    meterRegistry = new SimpleMeterRegistry();
    MetricUtils metricUtils = MetricUtils.builder().registry(meterRegistry).build();
    opContext =
        TestOperationContexts.systemContextTraceNoSearchAuthorization(
            null,
            () ->
                SystemTelemetryContext.builder()
                    .metricUtils(metricUtils)
                    .tracer(OpenTelemetry.noop().getTracer("test"))
                    .build());

    rollbackService =
        new RollbackService(
            mockEntityService,
            mockSystemMetadataService,
            mockTimeseriesAspectService,
            TEST_SYSTEM_METADATA_SERVICE_CONFIG.toBuilder()
                .limit(
                    TEST_SYSTEM_METADATA_SERVICE_CONFIG.getLimit().toBuilder()
                        .results(
                            ResultsLimitConfig.builder()
                                .max(MAX_SEARCH_RESULTS)
                                .apiDefault(MAX_SEARCH_RESULTS)
                                .build())
                        .build())
                .build());
  }

  @Test
  public void testMultiPageExecuteEmitsRowsPagesAndDuration() throws Exception {
    List<AspectRowSummary> firstPage = createAspectRows(TEST_URN_1, TEST_URN_2, true);
    // Pad to apiDefault so the while-loop runs a second page.
    while (firstPage.size() < MAX_SEARCH_RESULTS) {
      AspectRowSummary pad = new AspectRowSummary();
      pad.setUrn(TEST_URN_2);
      pad.setAspectName("datasetProperties" + firstPage.size());
      pad.setRunId(TEST_RUN_ID);
      pad.setKeyAspect(false);
      firstPage.add(pad);
    }
    List<AspectRowSummary> secondPage = createAspectRows(TEST_URN_1, TEST_URN_2, false);

    when(mockSystemMetadataService.findByRunId(
            any(OperationContext.class),
            eq(TEST_RUN_ID),
            eq(true),
            anyInt(),
            eq(MAX_SEARCH_RESULTS)))
        .thenReturn(firstPage)
        .thenReturn(secondPage)
        .thenReturn(new ArrayList<>());

    when(mockEntityService.rollbackRun(eq(opContext), anyList(), eq(TEST_RUN_ID), eq(true)))
        .thenReturn(new RollbackRunResult(firstPage, 0, rollbackResults(firstPage)))
        .thenReturn(new RollbackRunResult(secondPage, 0, rollbackResults(secondPage)));

    DeleteAspectValuesResult timeseriesResult = new DeleteAspectValuesResult();
    timeseriesResult.setNumDocsDeleted(0L);
    when(mockTimeseriesAspectService.rollbackTimeseriesAspects(eq(opContext), eq(TEST_RUN_ID)))
        .thenReturn(timeseriesResult);
    when(mockSystemMetadataService.findByUrn(
            any(OperationContext.class), anyString(), eq(false), eq(0), eq(MAX_SEARCH_RESULTS)))
        .thenReturn(new ArrayList<>());

    // Capture before rollbackIngestion: execute path mutates the list returned by
    // RollbackRunResult.getRowsRolledBack() (same instance as firstPage) via addAll.
    final int expectedRows = firstPage.size() + secondPage.size();

    rollbackService.rollbackIngestion(opContext, TEST_RUN_ID, false, true, null);

    Counter rows =
        meterRegistry
            .find(RollbackService.METRIC_PREFIX + ".rows_processed")
            .tag("operation_type", RollbackService.OPERATION_TYPE)
            .tag("phase", RollbackService.PHASE_EXECUTE)
            .counter();
    assertNotNull(rows);
    assertEquals(rows.count(), (double) expectedRows);

    Counter pages =
        meterRegistry
            .find(RollbackService.METRIC_PREFIX + ".pages")
            .tag("operation_type", RollbackService.OPERATION_TYPE)
            .tag("phase", RollbackService.PHASE_EXECUTE)
            .counter();
    assertNotNull(pages);
    assertEquals(pages.count(), 2.0);

    Counter launches =
        meterRegistry
            .find(RollbackService.METRIC_PREFIX + ".launches")
            .tag("operation_type", RollbackService.OPERATION_TYPE)
            .tag("phase", RollbackService.PHASE_EXECUTE)
            .counter();
    assertNotNull(launches);
    assertEquals(launches.count(), 1.0);

    Timer duration =
        meterRegistry
            .find(RollbackService.METRIC_PREFIX + ".duration")
            .tag("operation_type", RollbackService.OPERATION_TYPE)
            .tag("phase", RollbackService.PHASE_EXECUTE)
            .tag("status", "completed")
            .timer();
    assertNotNull(duration);
    assertEquals(duration.count(), 1L);
  }

  @Test
  public void testDryRunTaggedSeparately() throws Exception {
    List<AspectRowSummary> aspects = createAspectRows(TEST_URN_1, TEST_URN_2, true);
    when(mockSystemMetadataService.findByRunId(
            any(OperationContext.class), eq(TEST_RUN_ID), eq(false), eq(0), eq(MAX_SEARCH_RESULTS)))
        .thenReturn(aspects);
    when(mockSystemMetadataService.findByUrn(
            any(OperationContext.class), anyString(), eq(false), eq(0), eq(MAX_SEARCH_RESULTS)))
        .thenReturn(new ArrayList<>());

    rollbackService.rollbackIngestion(opContext, TEST_RUN_ID, true, false, null);

    Counter launches =
        meterRegistry
            .find(RollbackService.METRIC_PREFIX + ".launches")
            .tag("operation_type", RollbackService.OPERATION_TYPE)
            .tag("phase", RollbackService.PHASE_DRY_RUN)
            .counter();
    assertNotNull(launches, "dry_run must be a distinct phase series");
    assertEquals(launches.count(), 1.0);

    Timer duration =
        meterRegistry
            .find(RollbackService.METRIC_PREFIX + ".duration")
            .tag("phase", RollbackService.PHASE_DRY_RUN)
            .tag("status", "completed")
            .timer();
    assertNotNull(duration);
    assertEquals(duration.count(), 1L);
  }

  @Test
  public void testNullMetricUtilsStillRollsBack() throws Exception {
    OperationContext noMetrics =
        TestOperationContexts.systemContextTraceNoSearchAuthorization(
            null,
            () ->
                SystemTelemetryContext.builder()
                    .metricUtils(null)
                    .tracer(OpenTelemetry.noop().getTracer("test"))
                    .build());

    List<AspectRowSummary> aspects = createAspectRows(TEST_URN_1, TEST_URN_2, true);
    when(mockSystemMetadataService.findByRunId(
            any(OperationContext.class), eq(TEST_RUN_ID), eq(true), eq(0), eq(MAX_SEARCH_RESULTS)))
        .thenReturn(aspects)
        .thenReturn(new ArrayList<>());
    when(mockEntityService.rollbackRun(eq(noMetrics), anyList(), eq(TEST_RUN_ID), eq(true)))
        .thenReturn(new RollbackRunResult(aspects, 0, rollbackResults(aspects)));
    DeleteAspectValuesResult timeseriesResult = new DeleteAspectValuesResult();
    timeseriesResult.setNumDocsDeleted(0L);
    when(mockTimeseriesAspectService.rollbackTimeseriesAspects(eq(noMetrics), eq(TEST_RUN_ID)))
        .thenReturn(timeseriesResult);
    when(mockSystemMetadataService.findByUrn(
            any(OperationContext.class), anyString(), eq(false), eq(0), eq(MAX_SEARCH_RESULTS)))
        .thenReturn(new ArrayList<>());

    rollbackService.rollbackIngestion(noMetrics, TEST_RUN_ID, false, true, null);

    verify(mockEntityService, times(1)).rollbackRun(any(), anyList(), eq(TEST_RUN_ID), eq(true));
    assertTrue(meterRegistry.getMeters().isEmpty());
  }

  @Test
  public void testUnexpectedFailureRecordsFailedDuration() throws Exception {
    List<AspectRowSummary> aspects = createAspectRows(TEST_URN_1, TEST_URN_2, true);
    when(mockSystemMetadataService.findByRunId(
            any(OperationContext.class), eq(TEST_RUN_ID), eq(true), eq(0), eq(MAX_SEARCH_RESULTS)))
        .thenReturn(aspects);
    when(mockEntityService.rollbackRun(eq(opContext), anyList(), eq(TEST_RUN_ID), eq(true)))
        .thenThrow(new RuntimeException("boom"));

    try {
      rollbackService.rollbackIngestion(opContext, TEST_RUN_ID, false, true, null);
    } catch (RuntimeException expected) {
      assertEquals(expected.getMessage(), "boom");
    }

    Counter errors =
        meterRegistry
            .find(RollbackService.METRIC_PREFIX + ".errors")
            .tag("operation_type", RollbackService.OPERATION_TYPE)
            .tag("phase", RollbackService.PHASE_EXECUTE)
            .tag("error_type", "unexpected")
            .counter();
    assertNotNull(errors);
    assertEquals(errors.count(), 1.0);

    Timer duration =
        meterRegistry
            .find(RollbackService.METRIC_PREFIX + ".duration")
            .tag("phase", RollbackService.PHASE_EXECUTE)
            .tag("status", "failed")
            .timer();
    assertNotNull(duration);
    assertEquals(duration.count(), 1L);
  }

  private static List<AspectRowSummary> createAspectRows(
      String urn1, String urn2, boolean includeKeyAspects) {
    List<AspectRowSummary> rows = new ArrayList<>();
    if (includeKeyAspects) {
      AspectRowSummary key1 = new AspectRowSummary();
      key1.setUrn(urn1);
      key1.setAspectName("datasetKey");
      key1.setRunId(TEST_RUN_ID);
      key1.setKeyAspect(true);
      rows.add(key1);

      AspectRowSummary key2 = new AspectRowSummary();
      key2.setUrn(urn2);
      key2.setAspectName("datasetKey");
      key2.setRunId(TEST_RUN_ID);
      key2.setKeyAspect(true);
      rows.add(key2);
    }
    AspectRowSummary status1 = new AspectRowSummary();
    status1.setUrn(urn1);
    status1.setAspectName("status");
    status1.setRunId(TEST_RUN_ID);
    status1.setKeyAspect(false);
    rows.add(status1);

    AspectRowSummary status2 = new AspectRowSummary();
    status2.setUrn(urn2);
    status2.setAspectName("status");
    status2.setRunId(TEST_RUN_ID);
    status2.setKeyAspect(false);
    rows.add(status2);
    return rows;
  }

  private static List<RollbackResult> rollbackResults(List<AspectRowSummary> aspects) {
    List<RollbackResult> results = new ArrayList<>();
    for (AspectRowSummary aspect : aspects) {
      Urn urn = UrnUtils.getUrn(aspect.getUrn());
      results.add(
          new RollbackResult(
              urn,
              urn.getEntityType(),
              aspect.getAspectName(),
              null,
              null,
              null,
              null,
              ChangeType.UPSERT,
              Boolean.FALSE,
              0));
    }
    return results.isEmpty() ? Collections.emptyList() : results;
  }
}
