package com.linkedin.metadata.search.elasticsearch.indexbuilder;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.utils.elasticsearch.EntityIndexVersion;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.metadata.utils.elasticsearch.IndexConventionImpl;
import io.datahubproject.test.search.SearchTestUtils;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.testng.annotations.Test;

public class IncrementalReindexStateTest {

  private static final String ENTITY_INDEX = "datasetindex_v2";
  private static final String TIMESERIES_INDEX = "dataset_datasetprofileaspect_v1";
  private static final String NEXT_INDEX = "datasetindex_v2_next";
  private static final String OLD_BACKING = "datasetindex_v2_1779165194703";
  private static final String V3_INDEX = "primaryindex_v3";
  private static final String V3_OLD_BACKING = "primaryindex_v3_1779165194703";
  private static final IndexConvention INDEX_CONVENTION =
      IndexConventionImpl.noPrefix("md5", SearchTestUtils.DEFAULT_ENTITY_INDEX_CONFIGURATION);

  @Test
  public void testSetAndGetCatchUpStatus() {
    Map<String, String> state =
        IncrementalReindexState.setCatchUpStatus(
            null, ENTITY_INDEX, IncrementalReindexState.CatchUpStatus.COMPLETED);

    assertEquals(
        IncrementalReindexState.getCatchUpStatus(state, ENTITY_INDEX),
        Optional.of(IncrementalReindexState.CatchUpStatus.COMPLETED));
  }

  @Test
  public void testClearCatchUpStatusDropsResumeCursors() {
    Map<String, String> state =
        IncrementalReindexState.setCatchUpStatus(
            null, ENTITY_INDEX, IncrementalReindexState.CatchUpStatus.COMPLETED);
    state.put(ENTITY_INDEX + ".lastUrn", "urn:li:dataset:stale");
    state.put(ENTITY_INDEX + ".dataset.lastUrn", "urn:li:dataset:stale-type");
    state.put("chartindex_v2.lastUrn", "urn:li:chart:kept");

    Map<String, String> cleared = IncrementalReindexState.clearCatchUpStatus(state, ENTITY_INDEX);

    assertEquals(IncrementalReindexState.getCatchUpStatus(cleared, ENTITY_INDEX), Optional.empty());
    assertFalse(cleared.containsKey(ENTITY_INDEX + ".lastUrn"));
    assertFalse(cleared.containsKey(ENTITY_INDEX + ".dataset.lastUrn"));
    assertEquals(cleared.get("chartindex_v2.lastUrn"), "urn:li:chart:kept");
  }

  @Test
  public void testMergeCatchUpStateDropsAClearedVerdictAndItsCursors() {
    Map<String, String> loaded =
        IncrementalReindexState.setCatchUpStatus(
            null, ENTITY_INDEX, IncrementalReindexState.CatchUpStatus.COMPLETED);
    loaded.put(ENTITY_INDEX + ".lastUrn", "urn:li:dataset:stale");
    loaded.put(ENTITY_INDEX + ".dataset.lastUrn", "urn:li:dataset:stale-type");
    Map<String, String> updated = new HashMap<>();
    updated.put("chartindex_v2.lastUrn", "urn:li:chart:kept");

    Map<String, String> merged = IncrementalReindexState.mergeCatchUpState(loaded, updated);

    assertEquals(IncrementalReindexState.getCatchUpStatus(merged, ENTITY_INDEX), Optional.empty());
    assertFalse(merged.containsKey(ENTITY_INDEX + ".lastUrn"));
    assertFalse(merged.containsKey(ENTITY_INDEX + ".dataset.lastUrn"));
    assertEquals(merged.get("chartindex_v2.lastUrn"), "urn:li:chart:kept");
  }

  @Test
  public void testMergeCatchUpStateKeepsCursorsWhenTheStatusRemains() {
    Map<String, String> loaded = new HashMap<>();
    loaded.put(ENTITY_INDEX + ".lastUrn", "urn:li:dataset:kept");
    Map<String, String> updated =
        IncrementalReindexState.setCatchUpStatus(
            null, ENTITY_INDEX, IncrementalReindexState.CatchUpStatus.COMPLETED);

    Map<String, String> merged = IncrementalReindexState.mergeCatchUpState(loaded, updated);

    assertEquals(merged.get(ENTITY_INDEX + ".lastUrn"), "urn:li:dataset:kept");
    assertEquals(
        IncrementalReindexState.getCatchUpStatus(merged, ENTITY_INDEX),
        Optional.of(IncrementalReindexState.CatchUpStatus.COMPLETED));
  }

  @Test
  public void testCatchUpStatusTerminal() {
    assertTrue(IncrementalReindexState.CatchUpStatus.COMPLETED.isTerminal());
    assertTrue(IncrementalReindexState.CatchUpStatus.SKIPPED.isTerminal());
    assertFalse(IncrementalReindexState.CatchUpStatus.PENDING.isTerminal());
    assertFalse(IncrementalReindexState.CatchUpStatus.FAILED.isTerminal());
  }

  @Test
  public void testSetReindexCompleteTimeKeepsInProgress() {
    // Recording reindex (data copy) completion must NOT flip the status to COMPLETED — the index
    // stays IN_PROGRESS until the alias swap succeeds. Marking COMPLETED before the swap would make
    // a failed swap unrecoverable (resumed runs skip it instead of retrying the swap).
    Map<String, String> state =
        IncrementalReindexState.setPhase1State(
            null,
            ENTITY_INDEX,
            NEXT_INDEX,
            null,
            100L,
            0L,
            null,
            false,
            IncrementalReindexState.Status.IN_PROGRESS);

    state = IncrementalReindexState.setReindexCompleteTime(state, ENTITY_INDEX, 200L);

    assertEquals(
        IncrementalReindexState.get(
            state, ENTITY_INDEX, IncrementalReindexState.REINDEX_COMPLETE_TIME),
        Optional.of("200"));
    assertEquals(
        IncrementalReindexState.getStatus(state, ENTITY_INDEX),
        Optional.of(IncrementalReindexState.Status.IN_PROGRESS));
  }

  @Test
  public void testSetPhase1Completed() {
    // setPhase1Completed is what marks COMPLETED, and is only called after a successful alias swap.
    Map<String, String> state =
        IncrementalReindexState.setPhase1State(
            null,
            ENTITY_INDEX,
            NEXT_INDEX,
            null,
            100L,
            0L,
            null,
            false,
            IncrementalReindexState.Status.IN_PROGRESS);
    state = IncrementalReindexState.setReindexCompleteTime(state, ENTITY_INDEX, 200L);

    assertEquals(
        IncrementalReindexState.getStatus(state, ENTITY_INDEX),
        Optional.of(IncrementalReindexState.Status.IN_PROGRESS));

    state = IncrementalReindexState.setPhase1Completed(state, ENTITY_INDEX);

    assertEquals(
        IncrementalReindexState.getStatus(state, ENTITY_INDEX),
        Optional.of(IncrementalReindexState.Status.COMPLETED));
    // completion time is preserved across the status transition
    assertEquals(
        IncrementalReindexState.get(
            state, ENTITY_INDEX, IncrementalReindexState.REINDEX_COMPLETE_TIME),
        Optional.of("200"));
  }

  @Test
  public void testProtectsTimeseriesOldBackingWithGapAndNoCatchUpStatus() {
    Map<String, String> phase1State = timeseriesPhase1State(1000L, 2000L, OLD_BACKING);

    Set<String> protectedIndices = protectedIndices(phase1State, null);

    assertEquals(protectedIndices, Set.of(OLD_BACKING));
  }

  @Test
  public void testReleasesWhenCatchUpCompleted() {
    Map<String, String> phase1State = timeseriesPhase1State(1000L, 2000L, OLD_BACKING);
    Map<String, String> catchUpState =
        IncrementalReindexState.setCatchUpStatus(
            null, TIMESERIES_INDEX, IncrementalReindexState.CatchUpStatus.COMPLETED);

    Set<String> protectedIndices = protectedIndices(phase1State, catchUpState);

    assertTrue(protectedIndices.isEmpty());
  }

  @Test
  public void testTimeseriesReleasedWhenCatchUpCompletedAndRollbackEnabled() {
    Map<String, String> phase1State = timeseriesPhase1State(1000L, 2000L, OLD_BACKING);
    Map<String, String> catchUpState =
        IncrementalReindexState.setCatchUpStatus(
            null, TIMESERIES_INDEX, IncrementalReindexState.CatchUpStatus.COMPLETED);

    Set<String> protectedIndices =
        IncrementalReindexState.getProtectedPhysicalIndicesForCleanup(
            phase1State, catchUpState, INDEX_CONVENTION, true);

    assertTrue(protectedIndices.isEmpty());
  }

  @Test
  public void testEntityStillProtectedWhenCatchUpCompletedAndRollbackEnabled() {
    Map<String, String> phase1State = entityPhase1State(1000L, 2000L, OLD_BACKING);
    Map<String, String> catchUpState =
        IncrementalReindexState.setCatchUpStatus(
            null, ENTITY_INDEX, IncrementalReindexState.CatchUpStatus.COMPLETED);

    Set<String> protectedIndices =
        IncrementalReindexState.getProtectedPhysicalIndicesForCleanup(
            phase1State, catchUpState, INDEX_CONVENTION, true);

    assertEquals(protectedIndices, Set.of(OLD_BACKING));
  }

  @Test
  public void testEntityReleasedWhenCatchUpCompletedAndRollbackDisabled() {
    Map<String, String> phase1State = entityPhase1State(1000L, 2000L, OLD_BACKING);
    Map<String, String> catchUpState =
        IncrementalReindexState.setCatchUpStatus(
            null, ENTITY_INDEX, IncrementalReindexState.CatchUpStatus.COMPLETED);

    Set<String> protectedIndices =
        IncrementalReindexState.getProtectedPhysicalIndicesForCleanup(
            phase1State, catchUpState, INDEX_CONVENTION, false);

    assertTrue(protectedIndices.isEmpty());
  }

  @Test
  public void testReleasesWhenCatchUpSkipped() {
    Map<String, String> phase1State = timeseriesPhase1State(1000L, 2000L, OLD_BACKING);
    Map<String, String> catchUpState =
        IncrementalReindexState.setCatchUpStatus(
            null, TIMESERIES_INDEX, IncrementalReindexState.CatchUpStatus.SKIPPED);

    Set<String> protectedIndices = protectedIndices(phase1State, catchUpState);

    assertTrue(protectedIndices.isEmpty());
  }

  @Test
  public void testReleasesWhenDualWriteDisabled() {
    Map<String, String> phase1State = timeseriesPhase1State(1000L, 2000L, OLD_BACKING);
    phase1State = IncrementalReindexState.setDualWriteDisabled(phase1State, TIMESERIES_INDEX);

    Set<String> protectedIndices = protectedIndices(phase1State, null);

    assertTrue(protectedIndices.isEmpty());
  }

  @Test
  public void testReleasesWhenCatchUpWindowEmpty() {
    Map<String, String> phase1State = timeseriesPhase1State(2000L, 2000L, OLD_BACKING);

    Set<String> protectedIndices = protectedIndices(phase1State, null);

    assertTrue(protectedIndices.isEmpty());
  }

  @Test
  public void testEmptyPhase1StateReturnsEmptyProtectedSet() {
    assertTrue(
        IncrementalReindexState.getProtectedPhysicalIndicesForCleanup(
                null, null, INDEX_CONVENTION, false)
            .isEmpty());
    assertTrue(
        IncrementalReindexState.getProtectedPhysicalIndicesForCleanup(
                new HashMap<>(), null, INDEX_CONVENTION, false)
            .isEmpty());
  }

  @Test
  public void testMultipleIndicesProtectsOnlyPendingOnes() {
    String oldBacking2 = "chartindex_v2_old";
    Map<String, String> phase1State =
        IncrementalReindexState.setPhase1State(
            null,
            ENTITY_INDEX,
            NEXT_INDEX,
            OLD_BACKING,
            1000L,
            0L,
            null,
            true,
            IncrementalReindexState.Status.COMPLETED);
    phase1State = IncrementalReindexState.setDualWriteStartTime(phase1State, ENTITY_INDEX, 2000L);
    phase1State =
        IncrementalReindexState.setPhase1State(
            phase1State,
            "chartindex_v2",
            "chartindex_v2_next",
            oldBacking2,
            1000L,
            0L,
            null,
            true,
            IncrementalReindexState.Status.COMPLETED);
    phase1State =
        IncrementalReindexState.setDualWriteStartTime(phase1State, "chartindex_v2", 2000L);

    Map<String, String> catchUpState =
        IncrementalReindexState.setCatchUpStatus(
            null, ENTITY_INDEX, IncrementalReindexState.CatchUpStatus.COMPLETED);

    Set<String> protectedIndices =
        IncrementalReindexState.getProtectedPhysicalIndicesForCleanup(
            phase1State, catchUpState, INDEX_CONVENTION, false);

    assertEquals(protectedIndices, Set.of(oldBacking2));
  }

  @Test
  public void testIsCatchUpCompleteForAllIndices() {
    Map<String, String> phase1State = timeseriesPhase1State(1000L, 2000L, OLD_BACKING);
    assertFalse(IncrementalReindexState.isCatchUpCompleteForAllIndices(phase1State, null));

    Map<String, String> catchUpState =
        IncrementalReindexState.setCatchUpStatus(
            null, TIMESERIES_INDEX, IncrementalReindexState.CatchUpStatus.COMPLETED);
    assertTrue(IncrementalReindexState.isCatchUpCompleteForAllIndices(phase1State, catchUpState));
  }

  @Test
  public void testIsCatchUpCompleteForAllIndicesWhenPhase1Empty() {
    assertTrue(IncrementalReindexState.isCatchUpCompleteForAllIndices(null, null));
  }

  @Test
  public void testProtectsInProgressPhase1Status() {
    Map<String, String> phase1State =
        IncrementalReindexState.setPhase1State(
            null,
            TIMESERIES_INDEX,
            NEXT_INDEX,
            OLD_BACKING,
            1000L,
            0L,
            null,
            true,
            IncrementalReindexState.Status.IN_PROGRESS);
    phase1State =
        IncrementalReindexState.setDualWriteStartTime(phase1State, TIMESERIES_INDEX, 2000L);

    Set<String> protectedIndices = protectedIndices(phase1State, null);

    assertEquals(protectedIndices, Set.of(OLD_BACKING));
  }

  @Test
  public void testDoesNotProtectFailedPhase1Status() {
    Map<String, String> phase1State =
        IncrementalReindexState.setPhase1State(
            null,
            TIMESERIES_INDEX,
            NEXT_INDEX,
            OLD_BACKING,
            1000L,
            0L,
            null,
            true,
            IncrementalReindexState.Status.FAILED);
    phase1State =
        IncrementalReindexState.setDualWriteStartTime(phase1State, TIMESERIES_INDEX, 2000L);

    Set<String> protectedIndices = protectedIndices(phase1State, null);

    assertTrue(protectedIndices.isEmpty());
  }

  @Test
  public void testProtectsWhenCatchUpFailed() {
    Map<String, String> phase1State = timeseriesPhase1State(1000L, 2000L, OLD_BACKING);
    Map<String, String> catchUpState =
        IncrementalReindexState.setCatchUpStatus(
            null, TIMESERIES_INDEX, IncrementalReindexState.CatchUpStatus.FAILED);

    Set<String> protectedIndices = protectedIndices(phase1State, catchUpState);

    assertEquals(protectedIndices, Set.of(OLD_BACKING));
  }

  @Test
  public void testSkipsMissingOldBackingIndexName() {
    Map<String, String> phase1State =
        IncrementalReindexState.setPhase1State(
            null,
            TIMESERIES_INDEX,
            NEXT_INDEX,
            null,
            1000L,
            0L,
            null,
            true,
            IncrementalReindexState.Status.COMPLETED);
    phase1State =
        IncrementalReindexState.setDualWriteStartTime(phase1State, TIMESERIES_INDEX, 2000L);

    Set<String> protectedIndices = protectedIndices(phase1State, null);

    assertTrue(protectedIndices.isEmpty());
  }

  @Test
  public void testEntityReleasedWhenRollbackEnabledButIndexConventionNull() {
    Map<String, String> phase1State = entityPhase1State(1000L, 2000L, OLD_BACKING);
    Map<String, String> catchUpState =
        IncrementalReindexState.setCatchUpStatus(
            null, ENTITY_INDEX, IncrementalReindexState.CatchUpStatus.COMPLETED);

    Set<String> protectedIndices =
        IncrementalReindexState.getProtectedPhysicalIndicesForCleanup(
            phase1State, catchUpState, null, true);

    assertTrue(protectedIndices.isEmpty());
  }

  @Test
  public void testIndexVersionDefaultsToV2OnLegacyRows() {
    Map<String, String> state = entityPhase1State(1000L, 2000L, OLD_BACKING);
    assertEquals(
        IncrementalReindexState.getIndexVersion(state, ENTITY_INDEX), EntityIndexVersion.V2);

    state = IncrementalReindexState.setIndexVersion(state, ENTITY_INDEX, EntityIndexVersion.V3);
    assertEquals(
        IncrementalReindexState.getIndexVersion(state, ENTITY_INDEX), EntityIndexVersion.V3);
    assertEquals(
        IncrementalReindexState.getAllIndexStates(state)
            .get(ENTITY_INDEX)
            .get(IncrementalReindexState.INDEX_VERSION),
        "V3");
  }

  @Test
  public void testBackfillStateRoundTrip() {
    Map<String, String> state =
        IncrementalReindexState.setBackfillPending(null, V3_INDEX, V3_INDEX, 1000L);
    assertEquals(
        IncrementalReindexState.getStatus(state, V3_INDEX),
        Optional.of(IncrementalReindexState.Status.BACKFILL_PENDING));
    assertEquals(IncrementalReindexState.getBackfillCheckpoint(state, V3_INDEX), Optional.empty());
    assertEquals(IncrementalReindexState.getBackfillGeneration(state, V3_INDEX), Optional.empty());

    state = IncrementalReindexState.setBackfillGeneration(state, V3_INDEX, 7L);
    assertEquals(IncrementalReindexState.getBackfillGeneration(state, V3_INDEX), Optional.of(7L));

    IncrementalReindexState.BackfillCheckpoint checkpoint =
        new IncrementalReindexState.BackfillCheckpoint(
            "dataset", "urn:li:dataset:(urn:li:dataPlatform:x,b,PROD)", 250L, 1000L);
    state = IncrementalReindexState.setBackfillCheckpoint(state, V3_INDEX, checkpoint);
    assertEquals(
        IncrementalReindexState.getStatus(state, V3_INDEX),
        Optional.of(IncrementalReindexState.Status.BACKFILL_IN_PROGRESS));
    assertEquals(
        IncrementalReindexState.getBackfillCheckpoint(state, V3_INDEX), Optional.of(checkpoint));

    state = IncrementalReindexState.setBackfillCompleteTime(state, V3_INDEX, 5000L);
    assertEquals(
        IncrementalReindexState.getStatus(state, V3_INDEX),
        Optional.of(IncrementalReindexState.Status.BACKFILL_IN_PROGRESS));

    Map<String, String> row = IncrementalReindexState.getAllIndexStates(state).get(V3_INDEX);
    assertEquals(row.get(IncrementalReindexState.BACKFILL_TARGET_INDEX), V3_INDEX);
    assertEquals(row.get(IncrementalReindexState.BACKFILL_GENERATION), "7");
    assertEquals(row.get(IncrementalReindexState.BACKFILL_START_TIME), "1000");
    assertEquals(row.get(IncrementalReindexState.BACKFILL_CURSOR_ENTITY), "dataset");
    assertEquals(row.get(IncrementalReindexState.BACKFILL_PROCESSED_COUNT), "250");
    assertEquals(row.get(IncrementalReindexState.BACKFILL_COMPLETE_TIME), "5000");
  }

  @Test
  public void testFillInFlightProtectsItsTargetAndOldBackingIndex() {
    // Rebuild: the next physical index has no alias yet, so orphan cleanup would otherwise reap it
    // mid-fill; the old backing index is still live and is what the swap moves away from.
    String next = V3_INDEX + "_0_14_0-0_1779165194703";
    Map<String, String> rebuild =
        IncrementalReindexState.setPhase1State(
            null,
            V3_INDEX,
            next,
            V3_OLD_BACKING,
            1000L,
            0L,
            null,
            false,
            IncrementalReindexState.Status.BACKFILL_PENDING);
    rebuild = IncrementalReindexState.setBackfillPending(rebuild, V3_INDEX, next, 1000L);
    assertEquals(protectedIndices(rebuild, null), Set.of(next, V3_OLD_BACKING));

    // A fill straight into the alias protects only the physical index behind it; the alias name
    // itself is never a cleanup candidate.
    Map<String, String> fresh =
        IncrementalReindexState.setPhase1State(
            null,
            V3_INDEX,
            next,
            null,
            1000L,
            0L,
            null,
            false,
            IncrementalReindexState.Status.BACKFILL_PENDING);
    fresh = IncrementalReindexState.setBackfillPending(fresh, V3_INDEX, V3_INDEX, 1000L);
    assertEquals(protectedIndices(fresh, null), Set.of(next));

    // Protection holds while the fill runs and is released only by the ordinary rules afterwards.
    Map<String, String> inProgress =
        IncrementalReindexState.setBackfillCheckpoint(
            rebuild,
            V3_INDEX,
            new IncrementalReindexState.BackfillCheckpoint("dataset", null, 10L, 100L));
    assertEquals(protectedIndices(inProgress, null), Set.of(next, V3_OLD_BACKING));
  }

  @Test
  public void testBackfillCheckpointWithoutUrnClearsStaleCursor() {
    Map<String, String> state =
        IncrementalReindexState.setBackfillCheckpoint(
            new HashMap<>(),
            V3_INDEX,
            new IncrementalReindexState.BackfillCheckpoint("dataset", "urn:li:dataset:a", 1L, 2L));
    state =
        IncrementalReindexState.setBackfillCheckpoint(
            state, V3_INDEX, new IncrementalReindexState.BackfillCheckpoint("chart", null, 1L, 2L));

    IncrementalReindexState.BackfillCheckpoint resumed =
        IncrementalReindexState.getBackfillCheckpoint(state, V3_INDEX).orElseThrow();
    assertEquals(resumed.cursorEntity(), "chart");
    assertEquals(resumed.cursorUrn(), null);
  }

  @Test
  public void testV3EntityStillProtectedWhenCatchUpCompletedAndRollbackEnabled() {
    Map<String, String> phase1State =
        IncrementalReindexState.setPhase1State(
            null,
            V3_INDEX,
            V3_INDEX + "_next",
            V3_OLD_BACKING,
            1000L,
            0L,
            null,
            true,
            IncrementalReindexState.Status.COMPLETED);
    phase1State = IncrementalReindexState.setDualWriteStartTime(phase1State, V3_INDEX, 2000L);
    Map<String, String> catchUpState =
        IncrementalReindexState.setCatchUpStatus(
            null, V3_INDEX, IncrementalReindexState.CatchUpStatus.COMPLETED);

    Set<String> protectedIndices =
        IncrementalReindexState.getProtectedPhysicalIndicesForCleanup(
            phase1State, catchUpState, INDEX_CONVENTION, true);

    assertEquals(protectedIndices, Set.of(V3_OLD_BACKING));
  }

  @Test
  public void testBackfilledIndexAlwaysHasCatchUpWindow() {
    // Dual-write began before the fill started, which for a _reindex row would mean "no window".
    // A filled row still needs catch-up: pre-upgrade consumers wrote only the old generation.
    Map<String, String> phase1State =
        IncrementalReindexState.setPhase1State(
            null,
            V3_INDEX,
            V3_INDEX + "_next",
            V3_OLD_BACKING,
            2000L,
            0L,
            null,
            true,
            IncrementalReindexState.Status.COMPLETED);
    phase1State = IncrementalReindexState.setDualWriteStartTime(phase1State, V3_INDEX, 1000L);
    assertTrue(protectedIndices(phase1State, null).isEmpty());

    phase1State =
        IncrementalReindexState.setBackfillPending(phase1State, V3_INDEX, V3_INDEX, 1500L);
    phase1State = IncrementalReindexState.setPhase1Completed(phase1State, V3_INDEX);

    assertEquals(protectedIndices(phase1State, null), Set.of(V3_OLD_BACKING));
    assertFalse(IncrementalReindexState.isCatchUpCompleteForAllIndices(phase1State, null));

    Map<String, String> catchUpState =
        IncrementalReindexState.setCatchUpStatus(
            null, V3_INDEX, IncrementalReindexState.CatchUpStatus.COMPLETED);
    assertTrue(IncrementalReindexState.isCatchUpCompleteForAllIndices(phase1State, catchUpState));
  }

  private static Set<String> protectedIndices(
      Map<String, String> phase1State, Map<String, String> catchUpState) {
    return IncrementalReindexState.getProtectedPhysicalIndicesForCleanup(
        phase1State, catchUpState, INDEX_CONVENTION, false);
  }

  private static Map<String, String> entityPhase1State(
      long reindexStart, long dualWriteStart, String oldBacking) {
    Map<String, String> phase1State =
        IncrementalReindexState.setPhase1State(
            null,
            ENTITY_INDEX,
            NEXT_INDEX,
            oldBacking,
            reindexStart,
            0L,
            null,
            true,
            IncrementalReindexState.Status.COMPLETED);
    return IncrementalReindexState.setDualWriteStartTime(phase1State, ENTITY_INDEX, dualWriteStart);
  }

  @Test
  public void testAFillPlannedAfreshDoesNotInheritAnEarlierAttemptsProgress() {
    Map<String, String> failedAttempt = new java.util.HashMap<>();
    failedAttempt.put(
        IncrementalReindexState.key(ENTITY_INDEX, IncrementalReindexState.STATUS),
        IncrementalReindexState.Status.FAILED.name());
    failedAttempt.put(
        IncrementalReindexState.key(ENTITY_INDEX, IncrementalReindexState.BACKFILL_CURSOR_ENTITY),
        "dataset");
    failedAttempt.put(
        IncrementalReindexState.key(ENTITY_INDEX, IncrementalReindexState.BACKFILL_CURSOR_URN),
        "urn:li:dataset:(urn:li:dataPlatform:hive,last,PROD)");
    failedAttempt.put(
        IncrementalReindexState.key(ENTITY_INDEX, IncrementalReindexState.BACKFILL_URNS_PROCESSED),
        "7");
    failedAttempt.put(
        IncrementalReindexState.key(
            ENTITY_INDEX, IncrementalReindexState.OLD_BACKING_INDEX_ID_SCHEME_STALE),
        "true");
    failedAttempt.put(
        IncrementalReindexState.key(
            ENTITY_INDEX, IncrementalReindexState.OLD_BACKING_INDEX_INCOMPLETE_MIGRATION),
        "true");

    Map<String, String> replanned =
        IncrementalReindexState.setBackfillPending(failedAttempt, ENTITY_INDEX, NEXT_INDEX, 2000L);

    assertEquals(
        IncrementalReindexState.getStatus(replanned, ENTITY_INDEX),
        java.util.Optional.of(IncrementalReindexState.Status.BACKFILL_PENDING));
    assertTrue(IncrementalReindexState.getBackfillCheckpoint(replanned, ENTITY_INDEX).isEmpty());
    assertTrue(
        IncrementalReindexState.get(
                replanned, ENTITY_INDEX, IncrementalReindexState.BACKFILL_URNS_PROCESSED)
            .isEmpty());
    assertFalse(IncrementalReindexState.isOldBackingIndexIdSchemeStale(replanned, ENTITY_INDEX));
    assertFalse(
        IncrementalReindexState.isOldBackingIndexIncompleteMigration(replanned, ENTITY_INDEX));
  }

  private static Map<String, String> timeseriesPhase1State(
      long reindexStart, long dualWriteStart, String oldBacking) {
    Map<String, String> phase1State =
        IncrementalReindexState.setPhase1State(
            null,
            TIMESERIES_INDEX,
            NEXT_INDEX,
            oldBacking,
            reindexStart,
            0L,
            null,
            true,
            IncrementalReindexState.Status.COMPLETED);
    return IncrementalReindexState.setDualWriteStartTime(
        phase1State, TIMESERIES_INDEX, dualWriteStart);
  }
}
