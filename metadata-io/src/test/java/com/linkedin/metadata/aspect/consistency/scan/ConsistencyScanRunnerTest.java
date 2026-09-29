package com.linkedin.metadata.aspect.consistency.scan;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.metadata.aspect.consistency.ConsistencyService;
import com.linkedin.metadata.aspect.consistency.check.CheckBatchRequest;
import com.linkedin.metadata.aspect.consistency.check.CheckContext;
import com.linkedin.metadata.aspect.consistency.check.CheckResult;
import io.datahubproject.metadata.context.OperationContext;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ConsistencyScanRunnerTest {

  @Mock private ConsistencyService consistencyService;
  @Mock private OperationContext opContext;

  private ConsistencyScanRunner runner;

  @BeforeMethod
  public void setUp() {
    MockitoAnnotations.openMocks(this);
    runner = new ConsistencyScanRunner(consistencyService);
  }

  @Test
  public void testMultiBatchScan_checkpointsEveryBatch_progressThrottled() {
    when(consistencyService.countMatching(eq(opContext), any(CheckBatchRequest.class)))
        .thenReturn(Optional.of(300L));

    CheckResult batch1 =
        CheckResult.builder()
            .entitiesScanned(100)
            .issuesFound(0)
            .issues(List.of())
            .scrollId("s1")
            .build();
    CheckResult batch2 =
        CheckResult.builder()
            .entitiesScanned(100)
            .issuesFound(1)
            .issues(List.of())
            .scrollId("s2")
            .build();
    CheckResult batch3 =
        CheckResult.builder()
            .entitiesScanned(100)
            .issuesFound(0)
            .issues(List.of())
            .scrollId(null)
            .build();

    when(consistencyService.checkBatch(eq(opContext), any(CheckBatchRequest.class), isNull()))
        .thenReturn(batch1, batch2, batch3);

    List<ConsistencyScanCheckpoint> checkpoints = new ArrayList<>();
    AtomicInteger progressReports = new AtomicInteger();
    AtomicInteger batches = new AtomicInteger();

    ConsistencyScanResult result =
        runner.run(
            opContext,
            ConsistencyScanRequest.builder()
                .entityType("dataset")
                .batchSize(100)
                .progressLogIntervalMs(60_000)
                .progressWarmupMs(60_000) // suppress progress during short test
                .delayMs(0)
                .onBatch(
                    r -> {
                      batches.incrementAndGet();
                      return BatchHandleResult.none();
                    })
                .onCheckpoint(checkpoints::add)
                .onProgress(s -> progressReports.incrementAndGet())
                .build());

    assertEquals(batches.get(), 3);
    assertEquals(checkpoints.size(), 3);
    assertEquals(result.getEntitiesScanned(), 300);
    assertEquals(result.getTotalEstimate().longValue(), 300L);
    // Warmup not elapsed → no progress callbacks
    assertEquals(progressReports.get(), 0);
    verify(consistencyService, times(1)).countMatching(eq(opContext), any());
    verify(consistencyService, times(3)).checkBatch(eq(opContext), any(), isNull());
  }

  @Test
  public void testCountEmpty_rateOnlyNoTotal() {
    when(consistencyService.countMatching(eq(opContext), any(CheckBatchRequest.class)))
        .thenReturn(Optional.empty());
    when(consistencyService.checkBatch(eq(opContext), any(CheckBatchRequest.class), isNull()))
        .thenReturn(
            CheckResult.builder()
                .entitiesScanned(50)
                .issuesFound(0)
                .issues(List.of())
                .scrollId(null)
                .build());

    List<ConsistencyScanStart> starts = new ArrayList<>();
    ConsistencyScanResult result =
        runner.run(
            opContext,
            ConsistencyScanRequest.builder().entityType("dataset").onStart(starts::add).build());

    assertEquals(starts.size(), 1);
    assertFalse(starts.get(0).isEtaEnabled());
    assertNull(starts.get(0).getTotalEstimate());
    assertNull(result.getTotalEstimate());
    assertEquals(result.getEntitiesScanned(), 50);
  }

  @Test
  public void testOnStartUsesLimitedTotal() {
    when(consistencyService.countMatching(eq(opContext), any(CheckBatchRequest.class)))
        .thenReturn(Optional.of(1000L));
    when(consistencyService.checkBatch(eq(opContext), any(CheckBatchRequest.class), isNull()))
        .thenReturn(
            CheckResult.builder()
                .entitiesScanned(100)
                .issuesFound(0)
                .issues(List.of())
                .scrollId(null)
                .build());

    List<ConsistencyScanStart> starts = new ArrayList<>();
    runner.run(
        opContext,
        ConsistencyScanRequest.builder()
            .entityType("dataset")
            .limit(150)
            .onStart(starts::add)
            .build());

    assertEquals(starts.size(), 1);
    assertEquals(starts.get(0).getTotalEstimate().longValue(), 150L);
  }

  @Test
  public void testSharedContext_clearsOrphanUrnsEachBatch() {
    when(consistencyService.countMatching(eq(opContext), any(CheckBatchRequest.class)))
        .thenReturn(Optional.of(100L));
    when(consistencyService.checkBatch(eq(opContext), any(CheckBatchRequest.class), any()))
        .thenReturn(
            CheckResult.builder()
                .entitiesScanned(50)
                .issuesFound(0)
                .issues(List.of())
                .scrollId("more")
                .build(),
            CheckResult.builder()
                .entitiesScanned(50)
                .issuesFound(0)
                .issues(List.of())
                .scrollId(null)
                .build());

    CheckContext ctx = mock(CheckContext.class);
    runner.run(
        opContext,
        ConsistencyScanRequest.builder().entityType("dataset").checkContext(ctx).build());

    verify(ctx, times(2)).clearOrphanUrns("dataset");
    verify(consistencyService, times(2)).checkBatch(eq(opContext), any(), eq(ctx));
  }

  @Test
  public void testDelayInterruptStopsScan() throws Exception {
    when(consistencyService.countMatching(eq(opContext), any(CheckBatchRequest.class)))
        .thenReturn(Optional.of(1000L));
    when(consistencyService.checkBatch(eq(opContext), any(CheckBatchRequest.class), isNull()))
        .thenReturn(
            CheckResult.builder()
                .entitiesScanned(100)
                .issuesFound(0)
                .issues(List.of())
                .scrollId("more")
                .build());

    AtomicInteger batches = new AtomicInteger();
    AtomicInteger onCompleteCalls = new AtomicInteger();
    AtomicReference<ConsistencyScanResult> resultHolder = new AtomicReference<>();
    Thread runnerThread =
        new Thread(
            () ->
                resultHolder.set(
                    runner.run(
                        opContext,
                        ConsistencyScanRequest.builder()
                            .entityType("dataset")
                            .delayMs(60_000)
                            .onBatch(
                                r -> {
                                  batches.incrementAndGet();
                                  return BatchHandleResult.none();
                                })
                            .onComplete(r -> onCompleteCalls.incrementAndGet())
                            .build())));
    runnerThread.start();
    Thread.sleep(200);
    runnerThread.interrupt();
    runnerThread.join(5000);

    assertFalse(runnerThread.isAlive());
    assertTrue(resultHolder.get().isCancelled());
    assertEquals(batches.get(), 1);
    assertEquals(onCompleteCalls.get(), 0);
    verify(consistencyService, times(1)).checkBatch(eq(opContext), any(), isNull());
  }

  @Test
  public void testLimitStopsScan() {
    when(consistencyService.countMatching(eq(opContext), any(CheckBatchRequest.class)))
        .thenReturn(Optional.of(1000L));
    when(consistencyService.checkBatch(eq(opContext), any(CheckBatchRequest.class), isNull()))
        .thenAnswer(
            invocation -> {
              CheckBatchRequest batchRequest = invocation.getArgument(1);
              int scanned = batchRequest.getBatchSize();
              return CheckResult.builder()
                  .entitiesScanned(scanned)
                  .issuesFound(0)
                  .issues(List.of())
                  .scrollId("more")
                  .build();
            });

    ConsistencyScanResult result =
        runner.run(
            opContext, ConsistencyScanRequest.builder().entityType("dataset").limit(150).build());

    assertEquals(result.getEntitiesScanned(), 150);
    assertEquals(result.getTotalEstimate().longValue(), 150L); // min(count, limit)
    verify(consistencyService, times(2)).checkBatch(eq(opContext), any(), isNull());
  }

  @Test
  public void testOnBatchAccumulatesFixCounts() {
    when(consistencyService.countMatching(eq(opContext), any(CheckBatchRequest.class)))
        .thenReturn(Optional.of(100L));
    when(consistencyService.checkBatch(eq(opContext), any(CheckBatchRequest.class), isNull()))
        .thenReturn(
            CheckResult.builder()
                .entitiesScanned(100)
                .issuesFound(2)
                .issues(List.of())
                .scrollId(null)
                .build());

    ConsistencyScanResult result =
        runner.run(
            opContext,
            ConsistencyScanRequest.builder()
                .entityType("dataset")
                .onBatch(r -> BatchHandleResult.of(1, 1))
                .build());

    assertEquals(result.getIssuesFixed(), 1);
    assertEquals(result.getIssuesFailed(), 1);
    assertEquals(result.getIssuesFound(), 2);
  }

  @Test
  public void testEntityEtaIneligible_ignoresCount() {
    when(consistencyService.countMatching(eq(opContext), any(CheckBatchRequest.class)))
        .thenReturn(Optional.of(500L));
    when(consistencyService.checkBatch(eq(opContext), any(CheckBatchRequest.class), isNull()))
        .thenReturn(
            CheckResult.builder()
                .entitiesScanned(10)
                .issuesFound(0)
                .issues(List.of())
                .scrollId(null)
                .build());

    List<ConsistencyScanStart> starts = new ArrayList<>();
    ConsistencyScanResult result =
        runner.run(
            opContext,
            ConsistencyScanRequest.builder()
                .entityType("dataset")
                .entityEtaEligible(false)
                .onStart(starts::add)
                .build());

    assertFalse(starts.get(0).isEtaEnabled());
    assertNull(result.getTotalEstimate());
  }

  @Test
  public void testShouldStopHaltsEarly() {
    when(consistencyService.countMatching(eq(opContext), any(CheckBatchRequest.class)))
        .thenReturn(Optional.of(1000L));

    AtomicInteger calls = new AtomicInteger();
    when(consistencyService.checkBatch(eq(opContext), any(CheckBatchRequest.class), isNull()))
        .thenAnswer(
            inv -> {
              calls.incrementAndGet();
              return CheckResult.builder()
                  .entitiesScanned(100)
                  .issuesFound(0)
                  .issues(List.of())
                  .scrollId("more")
                  .build();
            });

    AtomicInteger batches = new AtomicInteger();
    runner.run(
        opContext,
        ConsistencyScanRequest.builder()
            .entityType("dataset")
            .shouldStop(() -> batches.get() >= 1)
            .onBatch(
                r -> {
                  batches.incrementAndGet();
                  return BatchHandleResult.none();
                })
            .build());

    // shouldStop checked at loop start: first iteration runs, second sees stop
    assertEquals(batches.get(), 1);
  }

  @Test
  public void testEmptyFirstBatch() {
    when(consistencyService.countMatching(eq(opContext), any(CheckBatchRequest.class)))
        .thenReturn(Optional.of(0L));
    when(consistencyService.checkBatch(eq(opContext), any(CheckBatchRequest.class), isNull()))
        .thenReturn(CheckResult.empty());

    ConsistencyScanResult result =
        runner.run(opContext, ConsistencyScanRequest.builder().entityType("dataset").build());

    assertEquals(result.getEntitiesScanned(), 0);
    assertEquals(result.getTotalEstimate().longValue(), 0L);
    assertTrue(result.getFinalProgress().isFinished());
    verify(consistencyService, times(1)).checkBatch(eq(opContext), any(), isNull());
  }
}
