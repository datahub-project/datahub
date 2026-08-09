package com.linkedin.metadata.entity.retention.buffer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.buffer.offload.HazelcastOffloadBuffer;
import com.linkedin.metadata.buffer.offload.OffloadDrainer;
import com.linkedin.metadata.config.offload.MergePolicy;
import com.linkedin.metadata.config.offload.SizingPolicy;
import com.linkedin.metadata.entity.RetentionService;
import com.linkedin.metadata.entity.retention.SimpleRetentionContextResolver;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

public class RetentionDrainerTest {

  private static final String MAP_NAME = "retention-pending";
  private static final String LOCK_MAP_NAME = "retention-drain-lock";
  private static final String SEQ_MAP_NAME = "retention-pending.seq";
  private static final Urn TEST_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:mysql,my_db.my_table,PROD)");
  private static final String ASPECT = "status";
  private static final String FAILED_ASPECT = "ownership";

  private static final OperationContext SYSTEM_CONTEXT =
      TestOperationContexts.systemContextNoSearchAuthorization();

  private HazelcastInstance hazelcastInstance;

  @AfterMethod
  public void tearDown() {
    if (hazelcastInstance != null) {
      hazelcastInstance.shutdown();
      hazelcastInstance = null;
    }
  }

  private static HazelcastInstance newIsolatedInstance() {
    Config config = new Config();
    config.setInstanceName("retention-drainer-test-" + UUID.randomUUID());
    config.setProperty("hazelcast.phone.home.enabled", "false");
    config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
    config.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(false);
    config.getNetworkConfig().getJoin().getAutoDetectionConfig().setEnabled(false);
    return Hazelcast.newHazelcastInstance(config);
  }

  private HazelcastOffloadBuffer<RetentionKey, Long> newBuffer() {
    return new HazelcastOffloadBuffer<>(
        hazelcastInstance,
        MAP_NAME,
        LOCK_MAP_NAME,
        SEQ_MAP_NAME,
        100_000,
        MergePolicy.KEEP_MAX_LONG,
        SizingPolicy.EVICT_LRU,
        CoalesceRetentionBuffer.drainOrder(),
        "retention",
        null);
  }

  private static RetentionKey key() {
    return new RetentionKey(TEST_URN.toString(), ASPECT);
  }

  private static RetentionDrainer newDrainer(
      HazelcastOffloadBuffer<RetentionKey, Long> buffer,
      RetentionService<?> retentionService,
      boolean enabled,
      MetricUtils metricUtils) {
    OffloadDrainer<RetentionKey, Long> delegate =
        new OffloadDrainer<>(
            buffer,
            new SimpleRetentionContextResolver(),
            SYSTEM_CONTEXT,
            new RetentionDrainAction(retentionService, metricUtils),
            10,
            60_000L,
            enabled,
            "retention",
            metricUtils,
            true, // backoff on (retention default)
            5L);
    return new RetentionDrainer(delegate);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testTickAppliesRetentionAndRemovesKeyOnSuccess() {
    hazelcastInstance = newIsolatedInstance();
    HazelcastOffloadBuffer<RetentionKey, Long> buffer = newBuffer();
    buffer.enqueue(key(), 3L);

    RetentionService<?> retentionService = mock(RetentionService.class);
    // Batch path returns the committed contexts so the drainer clears those keys.
    when(retentionService.applyRetentionBatchWithPolicyDefaults(any(), any()))
        .thenAnswer(invocation -> invocation.getArgument(1));
    RetentionDrainer drainer = newDrainer(buffer, retentionService, true, null);

    drainer.tick();

    verify(retentionService, times(1))
        .applyRetentionBatchWithPolicyDefaults(eq(SYSTEM_CONTEXT), any(List.class));
    assertTrue(buffer.drain(10).isEmpty());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testTickLeavesKeyForRetryOnFailure() {
    hazelcastInstance = newIsolatedInstance();
    HazelcastOffloadBuffer<RetentionKey, Long> buffer = newBuffer();
    buffer.enqueue(key(), 3L);

    RetentionService<?> retentionService = mock(RetentionService.class);
    doThrow(new RuntimeException("retention apply failed"))
        .when(retentionService)
        .applyRetentionBatchWithPolicyDefaults(any(), any());

    MetricUtils mockMetricUtils = mock(MetricUtils.class);
    RetentionDrainer drainer = newDrainer(buffer, retentionService, true, mockMetricUtils);

    drainer.tick();

    // Action threw → framework leaves the entry for retry (at-least-once). With backoff on, the
    // groupKey/resolveOpContext path would move it to limbo, but this is an ACTION failure (apply),
    // not a resolver failure, so backoff does NOT apply — the entry stays in the buffer.
    assertEquals(buffer.drain(10).size(), 1);
    verify(mockMetricUtils, times(1))
        .increment(eq(OffloadDrainer.class), eq("retention_action_failed"), eq(1.0d));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testTickRemovesOnlyCommittedKeysOnPartialSuccess() {
    // Two pending keys; the service commits only one (the other's per-context tx failed). Only the
    // committed key must be cleared via removeIfSame — the failed key stays for the next tick.
    hazelcastInstance = newIsolatedInstance();
    HazelcastOffloadBuffer<RetentionKey, Long> buffer = newBuffer();
    RetentionKey committedKey = new RetentionKey(TEST_URN.toString(), ASPECT);
    RetentionKey failedKey = new RetentionKey(TEST_URN.toString(), FAILED_ASPECT);
    buffer.enqueue(committedKey, 3L);
    buffer.enqueue(failedKey, 5L);

    RetentionService<?> retentionService = mock(RetentionService.class);
    // Return only the committed context, dropping the failed one — mirrors EbeanRetentionService
    // returning just the contexts whose own transaction committed.
    when(retentionService.applyRetentionBatchWithPolicyDefaults(any(), any()))
        .thenAnswer(
            invocation -> {
              List<RetentionService.RetentionContext> contexts = invocation.getArgument(1);
              return contexts.stream()
                  .filter(ctx -> ASPECT.equals(ctx.getAspectName()))
                  .collect(Collectors.toList());
            });
    RetentionDrainer drainer = newDrainer(buffer, retentionService, true, null);

    drainer.tick();

    List<Map.Entry<RetentionKey, Long>> remaining = buffer.drain(10);
    assertEquals(remaining.size(), 1);
    assertEquals(remaining.get(0).getKey(), failedKey);
    assertEquals(remaining.get(0).getValue(), Long.valueOf(5L));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testTickSkipsWhenDisabled() {
    hazelcastInstance = newIsolatedInstance();
    HazelcastOffloadBuffer<RetentionKey, Long> buffer = newBuffer();
    buffer.enqueue(key(), 3L);

    RetentionService<?> retentionService = mock(RetentionService.class);
    RetentionDrainer drainer = newDrainer(buffer, retentionService, false, null);

    drainer.tick();

    verify(retentionService, never()).applyRetentionBatchWithPolicyDefaults(any(), any());
    assertEquals(buffer.drain(10).size(), 1);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testTickSkipsWhenAnotherDrainerHoldsLock() {
    hazelcastInstance = newIsolatedInstance();
    HazelcastOffloadBuffer<RetentionKey, Long> buffer = newBuffer();
    buffer.enqueue(key(), 3L);
    Object heldToken = buffer.tryAcquireDrainLock("drain", Duration.ofSeconds(60));
    assertNotNull(heldToken);

    RetentionService<?> retentionService = mock(RetentionService.class);
    RetentionDrainer drainer = newDrainer(buffer, retentionService, true, null);

    drainer.tick();

    verify(retentionService, never()).applyRetentionBatchWithPolicyDefaults(any(), any());
    assertEquals(buffer.drain(10).size(), 1);

    buffer.releaseDrainLock("drain", heldToken);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testCoalesceRetentionBufferToDrainerAppliesCorrectContext() {
    // End-to-end Java path: CoalesceRetentionBuffer.enqueue (the adapter EntityServiceImpl calls)
    // → RetentionDrainer.tick() → RetentionService.apply with the exact (urn, aspect, maxVersion)
    // that was enqueued. No sleep, no @Scheduled, no docker — tick() is invoked directly.
    hazelcastInstance = newIsolatedInstance();
    HazelcastOffloadBuffer<RetentionKey, Long> buffer = newBuffer();
    CoalesceRetentionBuffer retentionBuffer =
        new CoalesceRetentionBuffer(buffer, new SimpleRetentionContextResolver());

    retentionBuffer.enqueue(SYSTEM_CONTEXT, TEST_URN, ASPECT, 3L);
    // keep-max coalesce: a lower re-merge must not win.
    retentionBuffer.enqueue(SYSTEM_CONTEXT, TEST_URN, ASPECT, 1L);

    RetentionService<?> retentionService = mock(RetentionService.class);
    when(retentionService.applyRetentionBatchWithPolicyDefaults(any(), any()))
        .thenAnswer(invocation -> invocation.getArgument(1));
    RetentionDrainer drainer = newDrainer(buffer, retentionService, true, null);

    drainer.tick();

    ArgumentCaptor<List<RetentionService.RetentionContext>> captor =
        ArgumentCaptor.forClass(List.class);
    verify(retentionService, times(1))
        .applyRetentionBatchWithPolicyDefaults(eq(SYSTEM_CONTEXT), captor.capture());
    List<RetentionService.RetentionContext> applied = captor.getValue();
    assertEquals(applied.size(), 1);
    assertEquals(applied.get(0).getUrn(), TEST_URN);
    assertEquals(applied.get(0).getAspectName(), ASPECT);
    assertEquals(applied.get(0).getMaxVersion().orElseThrow(), 3L);

    // removeIfSame on success must have cleared the key.
    assertTrue(buffer.drain(10).isEmpty());
  }
}
