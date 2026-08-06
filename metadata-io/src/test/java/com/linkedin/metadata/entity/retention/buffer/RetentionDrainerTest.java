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
import com.linkedin.metadata.EbeanTestUtils;
import com.linkedin.metadata.buffer.CoalesceBuffer;
import com.linkedin.metadata.buffer.CoalesceBuffers;
import com.linkedin.metadata.buffer.HazelcastCoalesceBuffer;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.RetentionService;
import com.linkedin.metadata.entity.ebean.EbeanRetentionService;
import com.linkedin.metadata.entity.ebean.PassThroughScopedTransactionFactory;
import com.linkedin.metadata.entity.ebean.PlainAspectTableResolver;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import io.ebean.Database;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

public class RetentionDrainerTest {

  private static final String MAP_NAME = "retention-pending";
  private static final String LOCK_MAP_NAME = "retention-drain-lock";
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

  private static RetentionKey key() {
    return new SimpleRetentionKey(TEST_URN.toString(), ASPECT);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testTickAppliesRetentionAndRemovesKeyOnSuccess() {
    hazelcastInstance = newIsolatedInstance();
    CoalesceBuffer<RetentionKey, Long> buffer =
        new HazelcastCoalesceBuffer<>(hazelcastInstance, MAP_NAME, LOCK_MAP_NAME, null);
    buffer.merge(key(), 3L, CoalesceBuffers.KEEP_MAX_LONG);

    RetentionService<?> retentionService = mock(RetentionService.class);
    // Batch path returns the committed keys so the drainer clears those keys.
    when(retentionService.applyRetentionBatchWithPolicyDefaults(any(), any(), any()))
        .thenAnswer(invocation -> invocation.getArgument(1));
    RetentionDrainer drainer =
        new RetentionDrainer(
            buffer,
            retentionService,
            SYSTEM_CONTEXT,
            new SimpleRetentionContextResolver(),
            10,
            60_000L,
            true,
            null);

    drainer.tick();

    verify(retentionService, times(1))
        .applyRetentionBatchWithPolicyDefaults(
            eq(SYSTEM_CONTEXT), any(List.class), any(List.class));
    assertTrue(buffer.drain(10).isEmpty());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testTickLeavesKeyForRetryOnFailure() {
    hazelcastInstance = newIsolatedInstance();
    CoalesceBuffer<RetentionKey, Long> buffer =
        new HazelcastCoalesceBuffer<>(hazelcastInstance, MAP_NAME, LOCK_MAP_NAME, null);
    buffer.merge(key(), 3L, CoalesceBuffers.KEEP_MAX_LONG);

    RetentionService<?> retentionService = mock(RetentionService.class);
    doThrow(new RuntimeException("retention apply failed"))
        .when(retentionService)
        .applyRetentionBatchWithPolicyDefaults(any(), any(), any());

    MetricUtils mockMetricUtils = mock(MetricUtils.class);
    RetentionDrainer drainer =
        new RetentionDrainer(
            buffer,
            retentionService,
            SYSTEM_CONTEXT,
            new SimpleRetentionContextResolver(),
            10,
            60_000L,
            true,
            mockMetricUtils);

    drainer.tick();

    assertTrue(buffer.drain(10).size() == 1);
    verify(mockMetricUtils, times(1))
        .increment(eq(RetentionDrainer.class), eq("retention_drain_failed"), eq(1.0d));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testTickRemovesOnlyCommittedKeysOnPartialSuccess() {
    // Two pending keys; the service commits only one (the other's per-context tx failed). Only the
    // committed key must be cleared via removeIfSame — the failed key stays for the next tick.
    hazelcastInstance = newIsolatedInstance();
    CoalesceBuffer<RetentionKey, Long> buffer =
        new HazelcastCoalesceBuffer<>(hazelcastInstance, MAP_NAME, LOCK_MAP_NAME, null);
    RetentionKey committedKey = new SimpleRetentionKey(TEST_URN.toString(), ASPECT);
    RetentionKey failedKey = new SimpleRetentionKey(TEST_URN.toString(), FAILED_ASPECT);
    buffer.merge(committedKey, 3L, CoalesceBuffers.KEEP_MAX_LONG);
    buffer.merge(failedKey, 5L, CoalesceBuffers.KEEP_MAX_LONG);

    RetentionService<?> retentionService = mock(RetentionService.class);
    // Return only the committed key, dropping the failed one — mirrors EbeanRetentionService
    // returning just the keys whose own transaction committed.
    when(retentionService.applyRetentionBatchWithPolicyDefaults(any(), any(), any()))
        .thenAnswer(
            invocation -> {
              @SuppressWarnings("unchecked")
              List<RetentionKey> keys = invocation.getArgument(1);
              return keys.stream()
                  .filter(k -> ASPECT.equals(k.aspectName()))
                  .collect(Collectors.toList());
            });
    RetentionDrainer drainer =
        new RetentionDrainer(
            buffer,
            retentionService,
            SYSTEM_CONTEXT,
            new SimpleRetentionContextResolver(),
            10,
            60_000L,
            true,
            null);

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
    CoalesceBuffer<RetentionKey, Long> buffer =
        new HazelcastCoalesceBuffer<>(hazelcastInstance, MAP_NAME, LOCK_MAP_NAME, null);
    buffer.merge(key(), 3L, CoalesceBuffers.KEEP_MAX_LONG);

    RetentionService<?> retentionService = mock(RetentionService.class);
    RetentionDrainer drainer =
        new RetentionDrainer(
            buffer,
            retentionService,
            SYSTEM_CONTEXT,
            new SimpleRetentionContextResolver(),
            10,
            60_000L,
            false,
            null);

    drainer.tick();

    verify(retentionService, never()).applyRetentionBatchWithPolicyDefaults(any(), any(), any());
    assertTrue(buffer.drain(10).size() == 1);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testTickSkipsWhenAnotherDrainerHoldsLock() {
    hazelcastInstance = newIsolatedInstance();
    CoalesceBuffer<RetentionKey, Long> buffer =
        new HazelcastCoalesceBuffer<>(hazelcastInstance, MAP_NAME, LOCK_MAP_NAME, null);
    buffer.merge(key(), 3L, CoalesceBuffers.KEEP_MAX_LONG);
    Object heldToken = buffer.tryAcquireDrainLock("drain", Duration.ofSeconds(60));
    assertNotNull(heldToken);

    RetentionService<?> retentionService = mock(RetentionService.class);
    RetentionDrainer drainer =
        new RetentionDrainer(
            buffer,
            retentionService,
            SYSTEM_CONTEXT,
            new SimpleRetentionContextResolver(),
            10,
            60_000L,
            true,
            null);

    drainer.tick();

    verify(retentionService, never()).applyRetentionBatchWithPolicyDefaults(any(), any(), any());
    assertTrue(buffer.drain(10).size() == 1);

    buffer.releaseDrainLock("drain", heldToken);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testCoalesceRetentionBufferToDrainerAppliesCorrectContext() {
    // End-to-end Java path: CoalesceRetentionBuffer.enqueue (the adapter EntityServiceImpl calls)
    // → RetentionDrainer.tick() → RetentionService.apply with the exact (urn, aspect, maxVersion)
    // that was enqueued. No sleep, no @Scheduled, no docker — tick() is invoked directly.
    hazelcastInstance = newIsolatedInstance();
    CoalesceBuffer<RetentionKey, Long> buffer =
        new HazelcastCoalesceBuffer<>(hazelcastInstance, MAP_NAME, LOCK_MAP_NAME, null);
    CoalesceRetentionBuffer retentionBuffer =
        new CoalesceRetentionBuffer(buffer, new SimpleRetentionContextResolver());

    retentionBuffer.enqueue(SYSTEM_CONTEXT, TEST_URN, ASPECT, 3L);
    // keep-max coalesce: a lower re-merge must not win.
    retentionBuffer.enqueue(SYSTEM_CONTEXT, TEST_URN, ASPECT, 1L);

    RetentionService<?> retentionService = mock(RetentionService.class);
    when(retentionService.applyRetentionBatchWithPolicyDefaults(any(), any(), any()))
        .thenAnswer(invocation -> invocation.getArgument(1));
    RetentionDrainer drainer =
        new RetentionDrainer(
            buffer,
            retentionService,
            SYSTEM_CONTEXT,
            new SimpleRetentionContextResolver(),
            10,
            60_000L,
            true,
            null);

    drainer.tick();

    ArgumentCaptor<List<RetentionService.RetentionContext>> captor =
        ArgumentCaptor.forClass(List.class);
    verify(retentionService, times(1))
        .applyRetentionBatchWithPolicyDefaults(
            eq(SYSTEM_CONTEXT), any(List.class), captor.capture());
    List<RetentionService.RetentionContext> applied = captor.getValue();
    assertEquals(applied.size(), 1);
    assertEquals(applied.get(0).getUrn(), TEST_URN);
    assertEquals(applied.get(0).getAspectName(), ASPECT);
    assertEquals(applied.get(0).getMaxVersion().orElseThrow(), 3L);

    // removeIfSame on success must have cleared the key.
    assertTrue(buffer.drain(10).isEmpty());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testTickWithRealEbeanServiceClearsNoPolicyKeysFromBuffer() {
    // Integration guard against the infinite re-drain bug. The drainer passes parallel (keys,
    // contexts) lists to applyRetentionBatchWithPolicyDefaults. The Ebean override rebuilds each
    // context with a resolved policy but MUST echo back the ORIGINAL keys (at the committed
    // index) as successes — else the drainer's successes.contains(originalKey) match fails and
    // committed keys re-drain forever. This wires the REAL EbeanRetentionService against H2 (no
    // mocked service) and asserts the buffer is empty after a single tick. If the contract breaks,
    // this test holds 1 entry (the re-drain symptom).
    hazelcastInstance = newIsolatedInstance();
    CoalesceBuffer<RetentionKey, Long> buffer =
        new HazelcastCoalesceBuffer<>(hazelcastInstance, MAP_NAME, LOCK_MAP_NAME, null);
    CoalesceRetentionBuffer retentionBuffer =
        new CoalesceRetentionBuffer(buffer, new SimpleRetentionContextResolver());

    Database server = EbeanTestUtils.createTestServer("RetentionDrainerRealEbean");
    EntityService<?> entityService = mock(EntityService.class);
    // getRetention -> getLatestAspects empty -> getRetention returns new Retention() (empty) ->
    // no-op DELETE, but the key is still committed and returned as a success.
    when(entityService.getLatestAspects(any(), any(), any())).thenReturn(Collections.emptyMap());
    EbeanRetentionService<?> realService =
        new EbeanRetentionService<>(
            entityService,
            server,
            2,
            new PlainAspectTableResolver(),
            new PassThroughScopedTransactionFactory(server));

    try {
      retentionBuffer.enqueue(SYSTEM_CONTEXT, TEST_URN, ASPECT, 3L);

      RetentionDrainer drainer =
          new RetentionDrainer(
              buffer,
              realService,
              SYSTEM_CONTEXT,
              new SimpleRetentionContextResolver(),
              10,
              60_000L,
              true,
              null);

      drainer.tick();

      // Committed key must be cleared via removeIfSame. If the Ebean override returned
      // reconstructed keys, successes.contains(originalKey) would be false and this would still
      // hold 1 entry -> the infinite re-drain symptom.
      assertTrue(buffer.drain(10).isEmpty());
    } finally {
      EbeanTestUtils.shutdownDatabase(server);
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testTickClearsOnlyCommittedGroupKeysOnPartialGroupFailure() {
    // Pins the cross-off-by-key invariant across groups. SimpleRetentionContextResolver groups
    // all keys to "default" (one group), so to exercise per-group isolation we inject a custom
    // resolver that groups by aspectName. Two aspects -> two groups. The service stub throws for
    // the group whose first key has aspect "ownership" (failed group) and succeeds for "status".
    // Only the "status" key must be cleared; the "ownership" key stays for retry.
    hazelcastInstance = newIsolatedInstance();
    CoalesceBuffer<RetentionKey, Long> buffer =
        new HazelcastCoalesceBuffer<>(hazelcastInstance, MAP_NAME, LOCK_MAP_NAME, null);
    RetentionKey statusKey = new SimpleRetentionKey(TEST_URN.toString(), ASPECT);
    RetentionKey ownershipKey = new SimpleRetentionKey(TEST_URN.toString(), FAILED_ASPECT);
    buffer.merge(statusKey, 3L, CoalesceBuffers.KEEP_MAX_LONG);
    buffer.merge(ownershipKey, 5L, CoalesceBuffers.KEEP_MAX_LONG);

    RetentionContextResolver groupByAspect =
        new RetentionContextResolver() {
          @Override
          @Nonnull
          public RetentionKey enrichKey(
              @Nonnull OperationContext opContext, @Nonnull Urn urn, @Nonnull String aspectName) {
            return new SimpleRetentionKey(urn.toString(), aspectName);
          }

          @Override
          @Nonnull
          public String groupKey(@Nonnull RetentionKey key) {
            return key.aspectName();
          }

          @Override
          @Nonnull
          public OperationContext resolveOpContext(
              @Nonnull RetentionKey key, @Nonnull OperationContext systemOperationContext) {
            return systemOperationContext;
          }
        };

    RetentionService<?> retentionService = mock(RetentionService.class);
    when(retentionService.applyRetentionBatchWithPolicyDefaults(any(), any(), any()))
        .thenAnswer(
            invocation -> {
              @SuppressWarnings("unchecked")
              List<RetentionKey> groupKeys = invocation.getArgument(1);
              // Fail the whole group when its first (only) key is the ownership one.
              if (!groupKeys.isEmpty() && FAILED_ASPECT.equals(groupKeys.get(0).aspectName())) {
                throw new RuntimeException("forced group failure");
              }
              return groupKeys;
            });

    RetentionDrainer drainer =
        new RetentionDrainer(
            buffer, retentionService, SYSTEM_CONTEXT, groupByAspect, 10, 60_000L, true, null);

    drainer.tick();

    List<Map.Entry<RetentionKey, Long>> remaining = buffer.drain(10);
    assertEquals(remaining.size(), 1);
    assertEquals(remaining.get(0).getKey(), ownershipKey);
    assertEquals(remaining.get(0).getValue(), Long.valueOf(5L));
  }
}
