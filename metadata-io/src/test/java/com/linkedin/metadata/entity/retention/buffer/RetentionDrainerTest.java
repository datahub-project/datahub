package com.linkedin.metadata.entity.retention.buffer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
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
import com.linkedin.metadata.entity.retention.RetentionBatchEntry;
import com.linkedin.metadata.entity.retention.RetentionContextResolver;
import com.linkedin.metadata.entity.retention.RetentionKey;
import com.linkedin.metadata.entity.retention.RetentionTestUtils;
import com.linkedin.metadata.entity.retention.SimpleRetentionContextResolver;
import com.linkedin.metadata.entity.retention.SimpleRetentionKey;
import com.linkedin.metadata.entity.retention.UnresolvableRetentionKeyException;
import com.linkedin.metadata.event.EventProducer;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import io.ebean.Database;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
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
    when(retentionService.applyRetentionBatchWithPolicyDefaults(any(), any()))
        .thenAnswer(
            invocation -> {
              @SuppressWarnings("unchecked")
              List<RetentionBatchEntry> entries = invocation.getArgument(1);
              return entries.stream().map(RetentionBatchEntry::key).collect(Collectors.toList());
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

    verify(retentionService, times(1))
        .applyRetentionBatchWithPolicyDefaults(eq(SYSTEM_CONTEXT), any(List.class));
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
        .applyRetentionBatchWithPolicyDefaults(any(), any());

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
    when(retentionService.applyRetentionBatchWithPolicyDefaults(any(), any()))
        .thenAnswer(
            invocation -> {
              @SuppressWarnings("unchecked")
              List<RetentionBatchEntry> entries = invocation.getArgument(1);
              return entries.stream()
                  .map(RetentionBatchEntry::key)
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

    verify(retentionService, never()).applyRetentionBatchWithPolicyDefaults(any(), any());
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

    verify(retentionService, never()).applyRetentionBatchWithPolicyDefaults(any(), any());
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
    when(retentionService.applyRetentionBatchWithPolicyDefaults(any(), any()))
        .thenAnswer(
            invocation -> {
              @SuppressWarnings("unchecked")
              List<RetentionBatchEntry> entries = invocation.getArgument(1);
              return entries.stream().map(RetentionBatchEntry::key).collect(Collectors.toList());
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

    ArgumentCaptor<List<RetentionBatchEntry>> captor = ArgumentCaptor.forClass(List.class);
    verify(retentionService, times(1))
        .applyRetentionBatchWithPolicyDefaults(eq(SYSTEM_CONTEXT), captor.capture());
    List<RetentionBatchEntry> applied = captor.getValue();
    assertEquals(applied.size(), 1);
    assertEquals(applied.get(0).context().getUrn(), TEST_URN);
    assertEquals(applied.get(0).context().getAspectName(), ASPECT);
    assertEquals(applied.get(0).context().getMaxVersion().orElseThrow(), 3L);

    // removeIfSame on success must have cleared the key.
    assertTrue(buffer.drain(10).isEmpty());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testTickWithRealEbeanServiceClearsNoPolicyKeysFromBuffer()
      throws java.net.URISyntaxException {
    // Integration guard against the infinite re-drain bug. The drainer passes a single
    // List<RetentionBatchEntry> (each entry pairs a key with its context) to
    // applyRetentionBatchWithPolicyDefaults. The Ebean override rebuilds each context with a
    // resolved policy but MUST echo back the ORIGINAL keys (at the committed index) as
    // successes — else the drainer's successes.contains(originalKey) match fails and
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
    // getRetention -> SystemEntityClient.batchGetV2 -> getEntitiesV2 empty -> getRetention returns
    // new Retention() (empty) -> no-op DELETE, but the key is still committed and returned as a
    // success.
    when(entityService.getEntitiesV2(any(), any(), any(), any(), anyBoolean()))
        .thenReturn(Collections.emptyMap());
    EbeanRetentionService<?> realService =
        new EbeanRetentionService<>(
            entityService,
            server,
            2,
            new PlainAspectTableResolver(),
            new PassThroughScopedTransactionFactory(server),
            RetentionTestUtils.systemEntityClient(
                entityService, mock(EventProducer.class), mock(MetricUtils.class)));

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
    when(retentionService.applyRetentionBatchWithPolicyDefaults(any(), any()))
        .thenAnswer(
            invocation -> {
              @SuppressWarnings("unchecked")
              List<RetentionBatchEntry> entries = invocation.getArgument(1);
              List<RetentionKey> groupKeys =
                  entries.stream().map(RetentionBatchEntry::key).collect(Collectors.toList());
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

  @Test
  @SuppressWarnings("unchecked")
  public void testTickNoOpsOnEmptyBuffer() {
    // Empty batch (no keys) must no-op: no service call, no exception, buffer stays empty.
    hazelcastInstance = newIsolatedInstance();
    CoalesceBuffer<RetentionKey, Long> buffer =
        new HazelcastCoalesceBuffer<>(hazelcastInstance, MAP_NAME, LOCK_MAP_NAME, null);

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

    verify(retentionService, never()).applyRetentionBatchWithPolicyDefaults(any(), any());
    assertTrue(buffer.drain(10).isEmpty());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testTickDropsKeyWhenResolverThrowsUnresolvable() {
    // A key the resolver can PERMANENTLY never resolve (UnresolvableRetentionKeyException — e.g. a
    // subtype the resolver does not produce, a wiring bug or a stale rolling-deploy entry) must be
    // dropped from the buffer, not retried forever — otherwise a poison key wedges the drainer in
    // an infinite re-throw loop. The drainer catches UnresolvableRetentionKeyException and removes
    // the key via removeIfSame so it doesn't re-throw every tick.
    hazelcastInstance = newIsolatedInstance();
    CoalesceBuffer<RetentionKey, Long> buffer =
        new HazelcastCoalesceBuffer<>(hazelcastInstance, MAP_NAME, LOCK_MAP_NAME, null);
    buffer.merge(key(), 3L, CoalesceBuffers.KEEP_MAX_LONG);

    RetentionContextResolver throwingResolver =
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
            throw new UnresolvableRetentionKeyException("forced permanent resolver failure");
          }

          @Override
          @Nonnull
          public OperationContext resolveOpContext(
              @Nonnull RetentionKey key, @Nonnull OperationContext systemOperationContext) {
            return systemOperationContext;
          }
        };

    RetentionService<?> retentionService = mock(RetentionService.class);
    RetentionDrainer drainer =
        new RetentionDrainer(
            buffer, retentionService, SYSTEM_CONTEXT, throwingResolver, 10, 60_000L, true, null);

    drainer.tick();

    // The key must have been dropped (not left for infinite retry) and the service never invoked.
    verify(retentionService, never()).applyRetentionBatchWithPolicyDefaults(any(), any());
    assertTrue(buffer.drain(10).isEmpty());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testTickLeavesKeyQueuedOnTransientResolverFailure() {
    // A TRANSIENT resolver failure (any RuntimeException other than
    // UnresolvableRetentionKeyException — e.g. a temporary lookup error) must NOT drop the key:
    // dropping would silently skip retention for that entry. The drainer moves the key to a
    // backoff limbo (removed from the buffer so other queued keys progress) and re-merges it after
    // TRANSIENT_BACKOFF_TICKS ticks for retry. This test proves the key survives the transient
    // failure — it is re-merged and applied once the transient condition clears.
    hazelcastInstance = newIsolatedInstance();
    CoalesceBuffer<RetentionKey, Long> buffer =
        new HazelcastCoalesceBuffer<>(hazelcastInstance, MAP_NAME, LOCK_MAP_NAME, null);
    buffer.merge(key(), 3L, CoalesceBuffers.KEEP_MAX_LONG);

    AtomicInteger groupCalls = new AtomicInteger(0);
    RetentionContextResolver transientlyFailingResolver =
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
            if (groupCalls.getAndIncrement() == 0) {
              throw new RuntimeException("forced transient resolver failure");
            }
            return "default";
          }

          @Override
          @Nonnull
          public OperationContext resolveOpContext(
              @Nonnull RetentionKey key, @Nonnull OperationContext systemOperationContext) {
            return systemOperationContext;
          }
        };

    RetentionService<?> retentionService = mock(RetentionService.class);
    when(retentionService.applyRetentionBatchWithPolicyDefaults(any(), any()))
        .thenAnswer(
            invocation -> {
              List<RetentionBatchEntry> entries = invocation.getArgument(1);
              return entries.stream().map(RetentionBatchEntry::key).collect(Collectors.toList());
            });
    RetentionDrainer drainer =
        new RetentionDrainer(
            buffer,
            retentionService,
            SYSTEM_CONTEXT,
            transientlyFailingResolver,
            10,
            60_000L,
            true,
            null);

    // Tick 1: transient failure → key moved to backoff limbo (out of the buffer), service never
    // invoked. Buffer is empty because the key is in backoff, not dropped.
    drainer.tick();
    verify(retentionService, never()).applyRetentionBatchWithPolicyDefaults(any(), any());
    assertTrue(buffer.drain(10).isEmpty());

    // Tick through the backoff window: the key is re-merged and, now that the resolver succeeds,
    // applied and cleared. The key survived the transient failure — it was not silently dropped.
    for (long i = 0; i < RetentionDrainer.TRANSIENT_BACKOFF_TICKS; i++) {
      drainer.tick();
    }
    verify(retentionService, times(1)).applyRetentionBatchWithPolicyDefaults(any(), any());
    assertTrue(buffer.drain(10).isEmpty());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testTickDropsGroupWhenResolveOpContextThrowsUnresolvable() {
    // resolveOpContext() permanent failure (UnresolvableRetentionKeyException) must drop the
    // WHOLE group (all keys that share the routing context), not retry forever. Mirrors the
    // groupKey() permanent-failure contract but exercises the distinct resolveOpContext() code
    // path in drainBatch.
    hazelcastInstance = newIsolatedInstance();
    CoalesceBuffer<RetentionKey, Long> buffer =
        new HazelcastCoalesceBuffer<>(hazelcastInstance, MAP_NAME, LOCK_MAP_NAME, null);
    buffer.merge(key(), 3L, CoalesceBuffers.KEEP_MAX_LONG);

    RetentionContextResolver throwingResolver =
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
            return "default";
          }

          @Override
          @Nonnull
          public OperationContext resolveOpContext(
              @Nonnull RetentionKey key, @Nonnull OperationContext systemOperationContext) {
            throw new UnresolvableRetentionKeyException(
                "forced permanent resolveOpContext failure");
          }
        };

    RetentionService<?> retentionService = mock(RetentionService.class);
    RetentionDrainer drainer =
        new RetentionDrainer(
            buffer, retentionService, SYSTEM_CONTEXT, throwingResolver, 10, 60_000L, true, null);

    drainer.tick();

    // The whole group must have been dropped (not left for infinite retry); service never invoked.
    verify(retentionService, never()).applyRetentionBatchWithPolicyDefaults(any(), any());
    assertTrue(buffer.drain(10).isEmpty());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testTickLeavesGroupQueuedOnTransientResolveOpContextFailure() {
    // resolveOpContext() transient failure (plain RuntimeException) must NOT drop the group:
    // the group's keys are moved to a backoff limbo (removed from the buffer so other queued keys
    // progress) and re-merged after TRANSIENT_BACKOFF_TICKS ticks for retry. Mirrors the groupKey()
    // transient contract but exercises the distinct resolveOpContext() code path. Proves the keys
    // survive the transient failure — they are re-merged and applied once the condition clears.
    hazelcastInstance = newIsolatedInstance();
    CoalesceBuffer<RetentionKey, Long> buffer =
        new HazelcastCoalesceBuffer<>(hazelcastInstance, MAP_NAME, LOCK_MAP_NAME, null);
    buffer.merge(key(), 3L, CoalesceBuffers.KEEP_MAX_LONG);

    AtomicInteger resolveCalls = new AtomicInteger(0);
    RetentionContextResolver transientlyFailingResolver =
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
            return "default";
          }

          @Override
          @Nonnull
          public OperationContext resolveOpContext(
              @Nonnull RetentionKey key, @Nonnull OperationContext systemOperationContext) {
            if (resolveCalls.getAndIncrement() == 0) {
              throw new RuntimeException("forced transient resolveOpContext failure");
            }
            return systemOperationContext;
          }
        };

    RetentionService<?> retentionService = mock(RetentionService.class);
    when(retentionService.applyRetentionBatchWithPolicyDefaults(any(), any()))
        .thenAnswer(
            invocation -> {
              List<RetentionBatchEntry> entries = invocation.getArgument(1);
              return entries.stream().map(RetentionBatchEntry::key).collect(Collectors.toList());
            });
    RetentionDrainer drainer =
        new RetentionDrainer(
            buffer,
            retentionService,
            SYSTEM_CONTEXT,
            transientlyFailingResolver,
            10,
            60_000L,
            true,
            null);

    // Tick 1: transient resolveOpContext failure → group moved to backoff limbo, service never
    // invoked, buffer empty.
    drainer.tick();
    verify(retentionService, never()).applyRetentionBatchWithPolicyDefaults(any(), any());
    assertTrue(buffer.drain(10).isEmpty());

    // Tick through the backoff window: the key is re-merged and, now that resolveOpContext
    // succeeds, applied and cleared. The key survived the transient failure.
    for (long i = 0; i < RetentionDrainer.TRANSIENT_BACKOFF_TICKS; i++) {
      drainer.tick();
    }
    verify(retentionService, times(1)).applyRetentionBatchWithPolicyDefaults(any(), any());
    assertTrue(buffer.drain(10).isEmpty());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testConcurrentEnqueueWhileDrainingKeepsNewerVersion() throws Exception {
    // removeIfSame invariant: drain() takes a non-destructive snapshot, the service applies, then
    // removeIfSame clears only if the value still matches. If a concurrent merge lands a higher
    // version between drain() and removeIfSame, removeIfSame(oldVersion) fails and the newer entry
    // survives for the next tick — no data loss. Timing is forced deterministically by blocking
    // the service on a latch so the concurrent merge lands while the drain is in flight.
    hazelcastInstance = newIsolatedInstance();
    CoalesceBuffer<RetentionKey, Long> buffer =
        new HazelcastCoalesceBuffer<>(hazelcastInstance, MAP_NAME, LOCK_MAP_NAME, null);
    buffer.merge(key(), 5L, CoalesceBuffers.KEEP_MAX_LONG);

    CountDownLatch serviceEntered = new CountDownLatch(1);
    CountDownLatch mergeLanded = new CountDownLatch(1);
    CountDownLatch serviceProceed = new CountDownLatch(1);

    RetentionService<?> retentionService = mock(RetentionService.class);
    when(retentionService.applyRetentionBatchWithPolicyDefaults(any(), any()))
        .thenAnswer(
            invocation -> {
              serviceEntered.countDown();
              // Wait for the background merge to land a higher version before returning success.
              mergeLanded.await(5, TimeUnit.SECONDS);
              serviceProceed.await(5, TimeUnit.SECONDS);
              @SuppressWarnings("unchecked")
              List<RetentionBatchEntry> entries = invocation.getArgument(1);
              return entries.stream().map(RetentionBatchEntry::key).collect(Collectors.toList());
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

    final ExecutorService pool = Executors.newFixedThreadPool(2);
    try {
      // tick() blocks inside the service apply (drain snapshot already taken).
      Future<?> drainFuture = pool.submit(() -> drainer.tick());

      // Wait until the service has been entered, then land a higher version for the same key.
      assertTrue(serviceEntered.await(5, TimeUnit.SECONDS));
      buffer.merge(key(), 10L, CoalesceBuffers.KEEP_MAX_LONG);
      mergeLanded.countDown();
      serviceProceed.countDown();

      drainFuture.get(10, TimeUnit.SECONDS);

      // The newer version (10) must have survived the first drain — removeIfSame(key, 5) did
      // not match the current value (10), so the entry stays for the next tick.
      List<Map.Entry<RetentionKey, Long>> remaining = buffer.drain(10);
      assertEquals(remaining.size(), 1);
      assertEquals(remaining.get(0).getValue(), Long.valueOf(10L));

      // Second tick drains the surviving newer entry. Use doAnswer().when() (not
      // when().thenAnswer())
      // so the re-stub does not invoke the first (latch) answer during recording — that invocation
      // would run the latch lambda with Mockito's null recording-args and NPE on entries.stream().
      doAnswer(
              invocation -> {
                @SuppressWarnings("unchecked")
                List<RetentionBatchEntry> entries = invocation.getArgument(1);
                return entries.stream().map(RetentionBatchEntry::key).collect(Collectors.toList());
              })
          .when(retentionService)
          .applyRetentionBatchWithPolicyDefaults(any(), any());
      drainer.tick();
      assertTrue(buffer.drain(10).isEmpty());
    } finally {
      pool.shutdownNow();
    }
  }
}
