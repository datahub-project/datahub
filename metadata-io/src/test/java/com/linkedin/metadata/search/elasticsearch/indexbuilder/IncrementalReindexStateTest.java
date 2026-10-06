package com.linkedin.metadata.search.elasticsearch.indexbuilder;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

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
