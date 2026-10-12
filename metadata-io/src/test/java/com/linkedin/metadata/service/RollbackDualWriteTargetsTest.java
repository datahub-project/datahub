package com.linkedin.metadata.service;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringMap;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.metadata.entity.upgrade.DataHubUpgradeResultStore;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.IncrementalReindexState;
import com.linkedin.metadata.utils.elasticsearch.EntityIndexVersion;
import com.linkedin.upgrade.DataHubUpgradeResult;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import org.testng.annotations.Test;

public class RollbackDualWriteTargetsTest {

  private static final String V2_INDEX = "datasetindex_v2";
  private static final String V2_OLD_BACKING = "datasetindex_v2_1779165194703";
  private static final String V3_INDEX = "primaryindex_v3";
  private static final String V3_OLD_BACKING = "primaryindex_v3_1779165194703";

  private final OperationContext opContext =
      TestOperationContexts.systemContextNoSearchAuthorization();
  private final Urn upgradeIdUrn =
      UrnUtils.getUrn("urn:li:dataHubUpgrade:BuildIndicesIncremental_test");

  @Test
  public void testTargetsInSelectsSwappedIndicesOfEitherGeneration() {
    Map<String, String> state =
        phase1(null, V2_INDEX, V2_OLD_BACKING, IncrementalReindexState.Status.COMPLETED);
    state = phase1(state, V3_INDEX, V3_OLD_BACKING, IncrementalReindexState.Status.COMPLETED);
    state = IncrementalReindexState.setIndexVersion(state, V3_INDEX, EntityIndexVersion.V3);
    state =
        phase1(
            state,
            "chartindex_v2",
            "chartindex_v2_1779165194703",
            IncrementalReindexState.Status.IN_PROGRESS);
    state = phase1(state, "dashboardindex_v2", null, IncrementalReindexState.Status.COMPLETED);
    state =
        phase1(
            state,
            "tagindex_v2",
            "tagindex_v2_1779165194703",
            IncrementalReindexState.Status.DUAL_WRITE_DISABLED);

    Map<String, RollbackDualWriteTargets.Target> targets =
        RollbackDualWriteTargets.targetsIn(state);

    assertEquals(targets.keySet(), Map.of(V2_INDEX, "", V3_INDEX, "").keySet());
    assertEquals(
        targets.get(V2_INDEX),
        new RollbackDualWriteTargets.Target(V2_INDEX, V2_OLD_BACKING, EntityIndexVersion.V2));
    assertEquals(
        targets.get(V3_INDEX),
        new RollbackDualWriteTargets.Target(V3_INDEX, V3_OLD_BACKING, EntityIndexVersion.V3));
  }

  @Test
  public void testLookupsAreByAliasAndGeneration() {
    RollbackDualWriteTargets targets = targets(v2Target(), v3Target());

    assertEquals(targets.oldBackingIndexFor(V2_INDEX), Optional.of(V2_OLD_BACKING));
    assertEquals(targets.oldBackingIndexFor(V3_INDEX), Optional.of(V3_OLD_BACKING));
    assertEquals(targets.oldBackingIndexFor(V2_OLD_BACKING), Optional.empty());
    assertTrue(targets.hasTargets(EntityIndexVersion.V2));
    assertTrue(targets.hasTargets(EntityIndexVersion.V3));

    targets.remove(V2_INDEX);

    assertFalse(targets.hasTargets(EntityIndexVersion.V2));
    assertTrue(targets.hasTargets(EntityIndexVersion.V3));
  }

  @Test
  public void testDualWriteStartRecordedOncePerIndexAndRetriedAfterFailure() {
    AtomicInteger calls = new AtomicInteger();
    AtomicInteger failuresLeft = new AtomicInteger(1);
    RollbackDualWriteTargets targets =
        new RollbackDualWriteTargets(
            Map.of(V3_INDEX, v3Target()),
            (indexName, startTime) -> {
              assertEquals(indexName, V3_INDEX);
              assertTrue(startTime > 0);
              calls.incrementAndGet();
              if (failuresLeft.getAndDecrement() > 0) {
                throw new IllegalStateException("persist failed");
              }
            },
            null);

    targets.recordDualWriteStarted(V3_INDEX);
    targets.recordDualWriteStarted(V3_INDEX);
    targets.recordDualWriteStarted(V3_INDEX);

    assertEquals(calls.get(), 2, "one failed attempt, one success, then nothing more");
  }

  @Test
  public void testPollRemovesTargetsMarkedDualWriteDisabled() throws Exception {
    Map<String, String> state =
        phase1(null, V2_INDEX, V2_OLD_BACKING, IncrementalReindexState.Status.DUAL_WRITE_DISABLED);
    state = phase1(state, V3_INDEX, V3_OLD_BACKING, IncrementalReindexState.Status.COMPLETED);
    DataHubUpgradeResultStore store = mock(DataHubUpgradeResultStore.class);
    when(store.readLatest(any(), eq(upgradeIdUrn))).thenReturn(enveloped(state));
    RollbackDualWriteTargets targets =
        new RollbackDualWriteTargets(
            Map.of(V2_INDEX, v2Target(), V3_INDEX, v3Target()),
            null,
            new RollbackDualWriteTargets.StateSource(opContext, store, upgradeIdUrn));

    assertTrue(targets.pollState());

    assertEquals(targets.oldBackingIndexFor(V2_INDEX), Optional.empty());
    assertEquals(targets.oldBackingIndexFor(V3_INDEX), Optional.of(V3_OLD_BACKING));
  }

  @Test
  public void testPollAddsTargetsSwappedAfterStartup() throws Exception {
    Map<String, String> state =
        phase1(null, V3_INDEX, V3_OLD_BACKING, IncrementalReindexState.Status.COMPLETED);
    state = IncrementalReindexState.setIndexVersion(state, V3_INDEX, EntityIndexVersion.V3);
    DataHubUpgradeResultStore store = mock(DataHubUpgradeResultStore.class);
    when(store.readLatest(any(), eq(upgradeIdUrn))).thenReturn(enveloped(state));
    RollbackDualWriteTargets targets =
        new RollbackDualWriteTargets(
            Map.of(),
            null,
            new RollbackDualWriteTargets.StateSource(opContext, store, upgradeIdUrn));
    assertFalse(targets.hasTargets(EntityIndexVersion.V3));

    assertTrue(targets.pollState());

    assertEquals(targets.oldBackingIndexFor(V3_INDEX), Optional.of(V3_OLD_BACKING));
  }

  @Test
  public void testPollerOutlivesAnInFlightFillAndStopsWhenNothingRemains() throws Exception {
    Map<String, String> filling =
        IncrementalReindexState.setBackfillPending(null, V3_INDEX, V3_INDEX + "_next", 100L);
    Map<String, String> swapped =
        phase1(null, V3_INDEX, V3_OLD_BACKING, IncrementalReindexState.Status.COMPLETED);
    Map<String, String> disabled =
        phase1(null, V3_INDEX, V3_OLD_BACKING, IncrementalReindexState.Status.DUAL_WRITE_DISABLED);
    DataHubUpgradeResultStore store = mock(DataHubUpgradeResultStore.class);
    when(store.readLatest(any(), eq(upgradeIdUrn)))
        .thenReturn(enveloped(filling), enveloped(swapped), enveloped(disabled));
    RollbackDualWriteTargets targets =
        new RollbackDualWriteTargets(
            Map.of(),
            null,
            new RollbackDualWriteTargets.StateSource(opContext, store, upgradeIdUrn));

    // Nothing to dual-write yet, but the fill may still swap: keep polling.
    assertTrue(targets.pollState());
    assertFalse(targets.hasTargets(EntityIndexVersion.V3));
    // The fill swapped: the target appears without a restart.
    assertTrue(targets.pollState());
    assertEquals(targets.oldBackingIndexFor(V3_INDEX), Optional.of(V3_OLD_BACKING));
    // Phase 2 retired it and no fill is in flight: nothing left to watch.
    assertFalse(targets.pollState());
    assertFalse(targets.hasTargets(EntityIndexVersion.V3));
  }

  @Test
  public void testFromUpgradeResultLoadsCurrentState() throws Exception {
    Map<String, String> state =
        phase1(null, V3_INDEX, V3_OLD_BACKING, IncrementalReindexState.Status.COMPLETED);
    state = IncrementalReindexState.setIndexVersion(state, V3_INDEX, EntityIndexVersion.V3);
    DataHubUpgradeResultStore store = mock(DataHubUpgradeResultStore.class);
    when(store.readLatest(any(), eq(upgradeIdUrn))).thenReturn(enveloped(state));

    RollbackDualWriteTargets targets =
        RollbackDualWriteTargets.fromUpgradeResult(
            new RollbackDualWriteTargets.StateSource(opContext, store, upgradeIdUrn), null);

    assertEquals(targets.oldBackingIndexFor(V3_INDEX), Optional.of(V3_OLD_BACKING));
    assertFalse(targets.hasTargets(EntityIndexVersion.V2));
  }

  private static RollbackDualWriteTargets targets(RollbackDualWriteTargets.Target... entries) {
    return new RollbackDualWriteTargets(
        List.of(entries).stream()
            .collect(
                java.util.stream.Collectors.toMap(
                    RollbackDualWriteTargets.Target::indexName, target -> target)),
        null,
        null);
  }

  private static RollbackDualWriteTargets.Target v2Target() {
    return new RollbackDualWriteTargets.Target(V2_INDEX, V2_OLD_BACKING, EntityIndexVersion.V2);
  }

  private static RollbackDualWriteTargets.Target v3Target() {
    return new RollbackDualWriteTargets.Target(V3_INDEX, V3_OLD_BACKING, EntityIndexVersion.V3);
  }

  private static Map<String, String> phase1(
      Map<String, String> existing,
      String indexName,
      String oldBackingIndexName,
      IncrementalReindexState.Status status) {
    return IncrementalReindexState.setPhase1State(
        existing,
        indexName,
        indexName + "_next",
        oldBackingIndexName,
        100L,
        0L,
        null,
        true,
        status);
  }

  private static EnvelopedAspect enveloped(Map<String, String> state) {
    DataHubUpgradeResult result = new DataHubUpgradeResult();
    result.setState(DataHubUpgradeState.SUCCEEDED);
    result.setResult(new StringMap(state));
    EnvelopedAspect aspect = new EnvelopedAspect();
    aspect.setValue(new Aspect(result.data()));
    return aspect;
  }
}
