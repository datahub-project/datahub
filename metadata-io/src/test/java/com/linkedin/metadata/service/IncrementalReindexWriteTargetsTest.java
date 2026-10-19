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
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.testng.annotations.Test;

public class IncrementalReindexWriteTargetsTest {

  private static final String V2_INDEX = "datasetindex_v2";
  private static final String V2_OLD_BACKING = "datasetindex_v2_1779165194703";
  private static final String V3_INDEX = "primaryindex_v3";
  private static final String V3_OLD_BACKING = "primaryindex_v3_1779165194703";

  private final OperationContext opContext =
      TestOperationContexts.systemContextNoSearchAuthorization();
  private final Urn upgradeIdUrn =
      UrnUtils.getUrn("urn:li:dataHubUpgrade:BuildIndicesIncremental_test");

  @Test
  public void testRollbackTargetsInSelectsSwappedIndicesOfEitherGeneration() {
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

    Map<String, IncrementalReindexWriteTargets.RollbackTarget> targets =
        IncrementalReindexWriteTargets.rollbackTargetsIn(state);

    assertEquals(targets.keySet(), Map.of(V2_INDEX, "", V3_INDEX, "").keySet());
    assertEquals(
        targets.get(V2_INDEX),
        new IncrementalReindexWriteTargets.RollbackTarget(
            V2_INDEX, V2_OLD_BACKING, EntityIndexVersion.V2));
    assertEquals(
        targets.get(V3_INDEX),
        new IncrementalReindexWriteTargets.RollbackTarget(
            V3_INDEX, V3_OLD_BACKING, EntityIndexVersion.V3));
  }

  @Test
  public void testLookupsAreByAliasAndGeneration() {
    IncrementalReindexWriteTargets targets = targets(v2Target(), v3Target());

    assertEquals(targets.oldBackingIndexFor(V2_INDEX), Optional.of(V2_OLD_BACKING));
    assertEquals(targets.oldBackingIndexFor(V3_INDEX), Optional.of(V3_OLD_BACKING));
    assertEquals(targets.oldBackingIndexFor(V2_OLD_BACKING), Optional.empty());
    assertTrue(targets.hasRollbackTargets(EntityIndexVersion.V2));
    assertTrue(targets.hasRollbackTargets(EntityIndexVersion.V3));

    targets.removeRollbackTarget(V2_INDEX);

    assertFalse(targets.hasRollbackTargets(EntityIndexVersion.V2));
    assertTrue(targets.hasRollbackTargets(EntityIndexVersion.V3));
  }

  @Test
  public void testDualWriteStartRecordedOncePerIndexAndRetriedAfterFailure() {
    AtomicInteger calls = new AtomicInteger();
    AtomicInteger failuresLeft = new AtomicInteger(1);
    IncrementalReindexWriteTargets targets =
        new IncrementalReindexWriteTargets(
            Map.of(V3_INDEX, v3Target()),
            Map.of(),
            (indexName, startTime) -> {
              assertEquals(indexName, V3_INDEX);
              assertTrue(startTime > 0);
              calls.incrementAndGet();
              if (failuresLeft.getAndDecrement() > 0) {
                throw new IllegalStateException("persist failed");
              }
            },
            null,
            true);

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
    IncrementalReindexWriteTargets targets =
        new IncrementalReindexWriteTargets(
            Map.of(V2_INDEX, v2Target(), V3_INDEX, v3Target()),
            Map.of(),
            null,
            new IncrementalReindexWriteTargets.StateSource(opContext, store, upgradeIdUrn),
            true);

    targets.pollState();

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
    IncrementalReindexWriteTargets targets =
        new IncrementalReindexWriteTargets(
            Map.of(),
            Map.of(),
            null,
            new IncrementalReindexWriteTargets.StateSource(opContext, store, upgradeIdUrn),
            true);
    assertFalse(targets.hasRollbackTargets(EntityIndexVersion.V3));

    targets.pollState();

    assertEquals(targets.oldBackingIndexFor(V3_INDEX), Optional.of(V3_OLD_BACKING));
  }

  @Test
  public void testPollKeepsFollowingTheStateAfterEveryTargetRetires() throws Exception {
    Map<String, String> filling =
        IncrementalReindexState.setBackfillPending(null, V3_INDEX, V3_INDEX + "_next", 100L);
    Map<String, String> swapped =
        phase1(null, V3_INDEX, V3_OLD_BACKING, IncrementalReindexState.Status.COMPLETED);
    Map<String, String> disabled =
        phase1(null, V3_INDEX, V3_OLD_BACKING, IncrementalReindexState.Status.DUAL_WRITE_DISABLED);
    Map<String, String> replanned =
        IncrementalReindexState.setBackfillPending(null, V3_INDEX, V3_INDEX + "_next2", 200L);
    DataHubUpgradeResultStore store = mock(DataHubUpgradeResultStore.class);
    when(store.readLatest(any(), eq(upgradeIdUrn)))
        .thenReturn(
            enveloped(filling), enveloped(swapped), enveloped(disabled), enveloped(replanned));
    IncrementalReindexWriteTargets targets =
        new IncrementalReindexWriteTargets(
            Map.of(),
            Map.of(),
            null,
            new IncrementalReindexWriteTargets.StateSource(opContext, store, upgradeIdUrn),
            true);

    // Nothing to dual-write yet, but the fill is in flight: live deletes reach the next index.
    targets.pollState();
    assertFalse(targets.hasRollbackTargets(EntityIndexVersion.V3));
    assertEquals(targets.fillTargetFor(V3_INDEX), Optional.of(V3_INDEX + "_next"));
    // The fill swapped: the target appears without a restart.
    targets.pollState();
    assertEquals(targets.oldBackingIndexFor(V3_INDEX), Optional.of(V3_OLD_BACKING));
    // Phase 2 retired it and nothing is in flight.
    targets.pollState();
    assertFalse(targets.hasRollbackTargets(EntityIndexVersion.V3));
    assertEquals(targets.fillTargetFor(V3_INDEX), Optional.empty());
    // A fill planned later for the same version is still seen: the poll never stops.
    targets.pollState();
    assertEquals(targets.fillTargetFor(V3_INDEX), Optional.of(V3_INDEX + "_next2"));
  }

  @Test
  public void testFromUpgradeResultLoadsCurrentState() throws Exception {
    Map<String, String> state =
        phase1(null, V3_INDEX, V3_OLD_BACKING, IncrementalReindexState.Status.COMPLETED);
    state = IncrementalReindexState.setIndexVersion(state, V3_INDEX, EntityIndexVersion.V3);
    DataHubUpgradeResultStore store = mock(DataHubUpgradeResultStore.class);
    when(store.readLatest(any(), eq(upgradeIdUrn))).thenReturn(enveloped(state));

    IncrementalReindexWriteTargets targets =
        IncrementalReindexWriteTargets.fromUpgradeResult(
            new IncrementalReindexWriteTargets.StateSource(opContext, store, upgradeIdUrn),
            null,
            true);

    assertEquals(targets.oldBackingIndexFor(V3_INDEX), Optional.of(V3_OLD_BACKING));
    assertFalse(targets.hasRollbackTargets(EntityIndexVersion.V2));
  }

  @Test
  public void testFillTargetsInSelectsRebuildsInFlight() {
    Map<String, String> state =
        IncrementalReindexState.setBackfillPending(null, V3_INDEX, V3_INDEX + "_next", 100L);
    // A fill writing the alias itself is already reached by every live write.
    state = IncrementalReindexState.setBackfillPending(state, "tagindex_v3", "tagindex_v3", 100L);
    // A swapped index is a rollback target, not a fill target.
    state = phase1(state, V2_INDEX, V2_OLD_BACKING, IncrementalReindexState.Status.COMPLETED);

    assertEquals(
        IncrementalReindexWriteTargets.fillTargetsIn(state), Map.of(V3_INDEX, V3_INDEX + "_next"));
  }

  @Test
  public void testPollMirrorsARebuildUntilItSwaps() throws Exception {
    Map<String, String> filling =
        IncrementalReindexState.setBackfillPending(null, V3_INDEX, V3_INDEX + "_next", 100L);
    Map<String, String> swapped =
        phase1(null, V3_INDEX, V3_OLD_BACKING, IncrementalReindexState.Status.COMPLETED);
    DataHubUpgradeResultStore store = mock(DataHubUpgradeResultStore.class);
    when(store.readLatest(any(), eq(upgradeIdUrn)))
        .thenReturn(enveloped(filling), enveloped(swapped));
    IncrementalReindexWriteTargets targets =
        new IncrementalReindexWriteTargets(
            Map.of(),
            Map.of(),
            null,
            new IncrementalReindexWriteTargets.StateSource(opContext, store, upgradeIdUrn),
            true);

    targets.pollState();
    assertEquals(targets.fillTargetFor(V3_INDEX), Optional.of(V3_INDEX + "_next"));
    // The fill swapped: live deletes stop going to the next index, which the alias now serves,
    // and the swapped-out index becomes the rollback target.
    targets.pollState();
    assertEquals(targets.fillTargetFor(V3_INDEX), Optional.empty());
    assertEquals(targets.oldBackingIndexFor(V3_INDEX), Optional.of(V3_OLD_BACKING));
  }

  @Test
  public void testRollbackDisabledStillMirrorsFillsButLoadsNoRollbackTarget() throws Exception {
    Map<String, String> state =
        IncrementalReindexState.setBackfillPending(null, V3_INDEX, V3_INDEX + "_next", 100L);
    state = phase1(state, V2_INDEX, V2_OLD_BACKING, IncrementalReindexState.Status.COMPLETED);
    DataHubUpgradeResultStore store = mock(DataHubUpgradeResultStore.class);
    when(store.readLatest(any(), eq(upgradeIdUrn))).thenReturn(enveloped(state));

    IncrementalReindexWriteTargets targets =
        IncrementalReindexWriteTargets.fromUpgradeResult(
            new IncrementalReindexWriteTargets.StateSource(opContext, store, upgradeIdUrn),
            null,
            false);

    assertEquals(targets.fillTargetFor(V3_INDEX), Optional.of(V3_INDEX + "_next"));
    assertEquals(targets.oldBackingIndexFor(V2_INDEX), Optional.empty());
    targets.pollState();
    assertEquals(
        targets.oldBackingIndexFor(V2_INDEX),
        Optional.empty(),
        "a poll does not load rollback targets either");
  }

  @Test
  public void testAFailedStateReadKeepsTheTargets() throws Exception {
    Map<String, String> filling =
        IncrementalReindexState.setBackfillPending(null, V3_INDEX, V3_INDEX + "_next", 100L);
    DataHubUpgradeResultStore store = mock(DataHubUpgradeResultStore.class);
    when(store.readLatest(any(), eq(upgradeIdUrn)))
        .thenReturn(enveloped(filling))
        .thenThrow(new RuntimeException("read timed out"))
        .thenReturn(enveloped(Map.of()));
    IncrementalReindexWriteTargets targets =
        new IncrementalReindexWriteTargets(
            Map.of(),
            Map.of(),
            null,
            new IncrementalReindexWriteTargets.StateSource(opContext, store, upgradeIdUrn),
            true);

    targets.pollState();
    assertEquals(targets.fillTargetFor(V3_INDEX), Optional.of(V3_INDEX + "_next"));
    // A read that fails is not a state with nothing in flight: the fill target stays and live
    // deletes keep reaching the index the fill is building.
    targets.pollState();
    assertEquals(targets.fillTargetFor(V3_INDEX), Optional.of(V3_INDEX + "_next"));
    // A state that was read and holds nothing retires it.
    targets.pollState();
    assertEquals(targets.fillTargetFor(V3_INDEX), Optional.empty());
  }

  @Test
  public void testRollbackTargetsInSkipsAnAbandonedMigration() {
    Map<String, String> state =
        phase1(null, V3_INDEX, V3_OLD_BACKING, IncrementalReindexState.Status.COMPLETED);
    state = IncrementalReindexState.setOldBackingIndexIncompleteMigration(state, V3_INDEX);

    assertEquals(IncrementalReindexWriteTargets.rollbackTargetsIn(state).keySet(), Set.of());
  }

  @Test
  public void testRollbackTargetsInSkipsAnIndexRebuiltUnderANewIdScheme() {
    Map<String, String> state =
        phase1(null, V3_INDEX, V3_OLD_BACKING, IncrementalReindexState.Status.COMPLETED);
    state = IncrementalReindexState.setOldBackingIndexIdSchemeStale(state, V3_INDEX);
    state = phase1(state, V2_INDEX, V2_OLD_BACKING, IncrementalReindexState.Status.COMPLETED);

    // The old index stores each entity under an id the running hasher does not produce: a mirrored
    // write would sit next to the old copy and a mirrored delete would miss it.
    assertEquals(
        IncrementalReindexWriteTargets.rollbackTargetsIn(state).keySet(), Set.of(V2_INDEX));
  }

  @Test
  public void testPollsOftenUntilThisVersionsStateIsReadAndWhileARebuildFills() throws Exception {
    Map<String, String> filling =
        IncrementalReindexState.setBackfillPending(null, V3_INDEX, V3_INDEX + "_next", 100L);
    Map<String, String> swapped =
        phase1(null, V3_INDEX, V3_OLD_BACKING, IncrementalReindexState.Status.COMPLETED);
    DataHubUpgradeResultStore store = mock(DataHubUpgradeResultStore.class);
    when(store.readLatest(any(), eq(upgradeIdUrn)))
        .thenReturn(
            (EnvelopedAspect) null, enveloped(Map.of()), enveloped(filling), enveloped(swapped));
    IncrementalReindexWriteTargets targets =
        new IncrementalReindexWriteTargets(
            Map.of(),
            Map.of(),
            null,
            new IncrementalReindexWriteTargets.StateSource(opContext, store, upgradeIdUrn),
            true);

    long beforeAnyRead = targets.nextPollDelaySeconds();
    // The upgrade has written nothing for this version yet.
    targets.pollState();
    long stillUnread = targets.nextPollDelaySeconds();
    // Phase 1 wrote its state and planned no fill.
    targets.pollState();
    long idle = targets.nextPollDelaySeconds();
    // A rebuild is filling.
    targets.pollState();
    long rebuilding = targets.nextPollDelaySeconds();
    // It swapped.
    targets.pollState();
    long afterSwap = targets.nextPollDelaySeconds();

    // Consumers can be serving before the upgrade writes any state for their version, and a fill
    // both starts and swaps while they serve; the poll runs often through both and settles once
    // the state has been read and nothing is in flight.
    assertEquals(stillUnread, beforeAnyRead);
    assertTrue(idle > beforeAnyRead, "a state with nothing in flight slows the poll down");
    assertEquals(rebuilding, beforeAnyRead);
    assertEquals(afterSwap, idle);
  }

  private static IncrementalReindexWriteTargets targets(
      IncrementalReindexWriteTargets.RollbackTarget... entries) {
    return new IncrementalReindexWriteTargets(
        List.of(entries).stream()
            .collect(
                java.util.stream.Collectors.toMap(
                    IncrementalReindexWriteTargets.RollbackTarget::indexName, target -> target)),
        Map.of(),
        null,
        null,
        true);
  }

  private static IncrementalReindexWriteTargets.RollbackTarget v2Target() {
    return new IncrementalReindexWriteTargets.RollbackTarget(
        V2_INDEX, V2_OLD_BACKING, EntityIndexVersion.V2);
  }

  private static IncrementalReindexWriteTargets.RollbackTarget v3Target() {
    return new IncrementalReindexWriteTargets.RollbackTarget(
        V3_INDEX, V3_OLD_BACKING, EntityIndexVersion.V3);
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
