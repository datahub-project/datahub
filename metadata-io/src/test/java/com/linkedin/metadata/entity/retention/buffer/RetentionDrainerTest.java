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
import static org.testng.Assert.assertTrue;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.buffer.CaffeineCoalesceBuffer;
import com.linkedin.metadata.buffer.CoalesceBuffer;
import com.linkedin.metadata.buffer.CoalesceBuffers;
import com.linkedin.metadata.buffer.HazelcastCoalesceBuffer;
import com.linkedin.metadata.entity.RetentionService;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

public class RetentionDrainerTest {

  private static final String MAP_NAME = "retention-pending";
  private static final String LOCK_MAP_NAME = "retention-drain-lock";
  private static final Urn TEST_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:mysql,my_db.my_table,PROD)");
  private static final String ASPECT = "status";

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

  private static RetentionKey key() {
    return new RetentionKey(TEST_URN.toString(), ASPECT);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testTickAppliesRetentionAndRemovesKeyOnSuccess() {
    hazelcastInstance = newIsolatedInstance();
    CoalesceBuffer<RetentionKey, Long> buffer =
        new HazelcastCoalesceBuffer<>(hazelcastInstance, MAP_NAME, LOCK_MAP_NAME);
    buffer.merge(key(), 3L, CoalesceBuffers.KEEP_MAX_LONG);

    RetentionService<?> retentionService = mock(RetentionService.class);
    // Batch path returns the committed contexts so the drainer clears those keys.
    when(retentionService.applyRetentionBatchWithPolicyDefaults(any(), any()))
        .thenAnswer(invocation -> invocation.getArgument(1));
    RetentionDrainer drainer =
        new RetentionDrainer(buffer, retentionService, SYSTEM_CONTEXT, 10, true, null);

    drainer.tick();

    verify(retentionService, times(1))
        .applyRetentionBatchWithPolicyDefaults(eq(SYSTEM_CONTEXT), any(List.class));
    assertTrue(buffer.drain(10).isEmpty());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testTickLeavesKeyForRetryOnFailure() {
    hazelcastInstance = newIsolatedInstance();
    CoalesceBuffer<RetentionKey, Long> buffer =
        new HazelcastCoalesceBuffer<>(hazelcastInstance, MAP_NAME, LOCK_MAP_NAME);
    buffer.merge(key(), 3L, CoalesceBuffers.KEEP_MAX_LONG);

    RetentionService<?> retentionService = mock(RetentionService.class);
    doThrow(new RuntimeException("retention apply failed"))
        .when(retentionService)
        .applyRetentionBatchWithPolicyDefaults(any(), any());

    MetricUtils mockMetricUtils = mock(MetricUtils.class);
    RetentionDrainer drainer =
        new RetentionDrainer(buffer, retentionService, SYSTEM_CONTEXT, 10, true, mockMetricUtils);

    drainer.tick();

    assertTrue(buffer.drain(10).size() == 1);
    verify(mockMetricUtils, times(1))
        .increment(eq(RetentionDrainer.class), eq("retention_drain_failed"), eq(1.0d));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testTickSkipsWhenDisabled() {
    hazelcastInstance = newIsolatedInstance();
    CoalesceBuffer<RetentionKey, Long> buffer =
        new HazelcastCoalesceBuffer<>(hazelcastInstance, MAP_NAME, LOCK_MAP_NAME);
    buffer.merge(key(), 3L, CoalesceBuffers.KEEP_MAX_LONG);

    RetentionService<?> retentionService = mock(RetentionService.class);
    RetentionDrainer drainer =
        new RetentionDrainer(buffer, retentionService, SYSTEM_CONTEXT, 10, false, null);

    drainer.tick();

    verify(retentionService, never()).applyRetentionBatchWithPolicyDefaults(any(), any());
    assertTrue(buffer.drain(10).size() == 1);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testTickSkipsWhenAnotherDrainerHoldsLock() {
    hazelcastInstance = newIsolatedInstance();
    CoalesceBuffer<RetentionKey, Long> buffer =
        new HazelcastCoalesceBuffer<>(hazelcastInstance, MAP_NAME, LOCK_MAP_NAME);
    buffer.merge(key(), 3L, CoalesceBuffers.KEEP_MAX_LONG);
    assertTrue(buffer.tryAcquireDrainLock("drain", Duration.ofSeconds(60)));

    RetentionService<?> retentionService = mock(RetentionService.class);
    RetentionDrainer drainer =
        new RetentionDrainer(buffer, retentionService, SYSTEM_CONTEXT, 10, true, null);

    drainer.tick();

    verify(retentionService, never()).applyRetentionBatchWithPolicyDefaults(any(), any());
    assertTrue(buffer.drain(10).size() == 1);

    buffer.releaseDrainLock("drain");
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testCoalesceRetentionBufferToDrainerAppliesCorrectContext() {
    // End-to-end Java path: CoalesceRetentionBuffer.enqueue (the adapter EntityServiceImpl calls)
    // → RetentionDrainer.tick() → RetentionService.apply with the exact (urn, aspect, maxVersion)
    // that was enqueued. No sleep, no @Scheduled, no docker — tick() is invoked directly.
    CoalesceBuffer<RetentionKey, Long> caffeine =
        new CaffeineCoalesceBuffer<>("retention-pending", 100, null);
    CoalesceRetentionBuffer retentionBuffer = new CoalesceRetentionBuffer(caffeine);

    retentionBuffer.enqueue(TEST_URN, ASPECT, 3L);
    // keep-max coalesce: a lower re-merge must not win.
    retentionBuffer.enqueue(TEST_URN, ASPECT, 1L);

    RetentionService<?> retentionService = mock(RetentionService.class);
    when(retentionService.applyRetentionBatchWithPolicyDefaults(any(), any()))
        .thenAnswer(invocation -> invocation.getArgument(1));
    RetentionDrainer drainer =
        new RetentionDrainer(
            retentionBuffer.getCoalesceBuffer(), retentionService, SYSTEM_CONTEXT, 10, true, null);

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
    assertTrue(caffeine.drain(10).isEmpty());
  }
}
