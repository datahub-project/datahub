package com.linkedin.datahub.upgrade.system.elasticsearch;

import static org.testng.Assert.*;

import com.linkedin.metadata.search.elasticsearch.indexbuilder.IncrementalReindexState;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.testng.annotations.Test;

public class IncrementalReindexStateTest {

  private static final String INDEX_NAME = "datasetindex_v2";
  private static final String NEXT_INDEX = "datasetindex_v2_next_1679000000000";

  @Test
  public void testKey() {
    assertEquals(
        IncrementalReindexState.key(INDEX_NAME, IncrementalReindexState.NEXT_INDEX_NAME),
        "datasetindex_v2.nextIndexName");
    assertEquals(
        IncrementalReindexState.key(INDEX_NAME, IncrementalReindexState.STATUS),
        "datasetindex_v2.status");
  }

  @Test
  public void testGetFromNullMap() {
    assertEquals(
        IncrementalReindexState.get(null, INDEX_NAME, IncrementalReindexState.STATUS),
        Optional.empty());
  }

  @Test
  public void testGetMissingKey() {
    Map<String, String> map = new HashMap<>();
    assertEquals(
        IncrementalReindexState.get(map, INDEX_NAME, IncrementalReindexState.STATUS),
        Optional.empty());
  }

  @Test
  public void testSetPhase1StateAndRead() {
    Map<String, String> state =
        IncrementalReindexState.setPhase1State(
            null,
            INDEX_NAME,
            NEXT_INDEX,
            null,
            1679000000000L,
            true,
            IncrementalReindexState.Status.IN_PROGRESS);

    assertEquals(
        IncrementalReindexState.get(state, INDEX_NAME, IncrementalReindexState.NEXT_INDEX_NAME),
        Optional.of(NEXT_INDEX));
    assertEquals(
        IncrementalReindexState.get(state, INDEX_NAME, IncrementalReindexState.REINDEX_START_TIME),
        Optional.of("1679000000000"));
    assertEquals(
        IncrementalReindexState.get(
            state, INDEX_NAME, IncrementalReindexState.REQUIRES_DATA_BACKFILL),
        Optional.of("true"));
    assertEquals(
        IncrementalReindexState.getStatus(state, INDEX_NAME),
        Optional.of(IncrementalReindexState.Status.IN_PROGRESS));
  }

  @Test
  public void testSetPhase1StatePreservesExisting() {
    Map<String, String> existing = new HashMap<>();
    existing.put("someOtherKey", "someValue");

    Map<String, String> state =
        IncrementalReindexState.setPhase1State(
            existing,
            INDEX_NAME,
            NEXT_INDEX,
            null,
            100L,
            false,
            IncrementalReindexState.Status.PENDING);

    assertEquals(state.get("someOtherKey"), "someValue");
    assertEquals(
        IncrementalReindexState.getStatus(state, INDEX_NAME),
        Optional.of(IncrementalReindexState.Status.PENDING));
  }

  @Test
  public void testSetReindexCompleteTime() {
    Map<String, String> state =
        IncrementalReindexState.setPhase1State(
            null,
            INDEX_NAME,
            NEXT_INDEX,
            null,
            100L,
            false,
            IncrementalReindexState.Status.IN_PROGRESS);

    state = IncrementalReindexState.setReindexCompleteTime(state, INDEX_NAME, 200L);

    assertEquals(
        IncrementalReindexState.get(
            state, INDEX_NAME, IncrementalReindexState.REINDEX_COMPLETE_TIME),
        Optional.of("200"));
    assertEquals(
        IncrementalReindexState.getStatus(state, INDEX_NAME),
        Optional.of(IncrementalReindexState.Status.COMPLETED));
  }

  @Test
  public void testSetDualWriteStartTime() {
    Map<String, String> state =
        IncrementalReindexState.setPhase1State(
            null,
            INDEX_NAME,
            NEXT_INDEX,
            null,
            100L,
            false,
            IncrementalReindexState.Status.COMPLETED);

    state = IncrementalReindexState.setDualWriteStartTime(state, INDEX_NAME, 300L);

    assertEquals(
        IncrementalReindexState.get(
            state, INDEX_NAME, IncrementalReindexState.DUAL_WRITE_START_TIME),
        Optional.of("300"));
    // Status should not change
    assertEquals(
        IncrementalReindexState.getStatus(state, INDEX_NAME),
        Optional.of(IncrementalReindexState.Status.COMPLETED));
  }

  @Test
  public void testSetDualWriteDisabled() {
    Map<String, String> state =
        IncrementalReindexState.setPhase1State(
            null,
            INDEX_NAME,
            NEXT_INDEX,
            null,
            100L,
            false,
            IncrementalReindexState.Status.COMPLETED);

    state = IncrementalReindexState.setDualWriteDisabled(state, INDEX_NAME);

    assertEquals(
        IncrementalReindexState.getStatus(state, INDEX_NAME),
        Optional.of(IncrementalReindexState.Status.DUAL_WRITE_DISABLED));
  }

  @Test
  public void testGetAllIndexStatesEmpty() {
    Map<String, Map<String, String>> result = IncrementalReindexState.getAllIndexStates(null);
    assertTrue(result.isEmpty());

    result = IncrementalReindexState.getAllIndexStates(new HashMap<>());
    assertTrue(result.isEmpty());
  }

  @Test
  public void testGetAllIndexStatesMultipleIndices() {
    Map<String, String> state =
        IncrementalReindexState.setPhase1State(
            null,
            "datasetindex_v2",
            "dataset_next_1",
            null,
            100L,
            true,
            IncrementalReindexState.Status.COMPLETED);
    state =
        IncrementalReindexState.setPhase1State(
            state,
            "chartindex_v2",
            "chart_next_1",
            null,
            200L,
            false,
            IncrementalReindexState.Status.IN_PROGRESS);

    Map<String, Map<String, String>> allStates = IncrementalReindexState.getAllIndexStates(state);

    assertEquals(allStates.size(), 2);
    assertTrue(allStates.containsKey("datasetindex_v2"));
    assertTrue(allStates.containsKey("chartindex_v2"));

    assertEquals(allStates.get("datasetindex_v2").get(IncrementalReindexState.STATUS), "COMPLETED");
    assertEquals(
        allStates.get("datasetindex_v2").get(IncrementalReindexState.REQUIRES_DATA_BACKFILL),
        "true");

    assertEquals(allStates.get("chartindex_v2").get(IncrementalReindexState.STATUS), "IN_PROGRESS");
    assertEquals(
        allStates.get("chartindex_v2").get(IncrementalReindexState.REINDEX_START_TIME), "200");
  }
}
