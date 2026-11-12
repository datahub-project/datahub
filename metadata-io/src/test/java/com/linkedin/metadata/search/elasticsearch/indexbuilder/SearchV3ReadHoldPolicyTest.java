package com.linkedin.metadata.search.elasticsearch.indexbuilder;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.testng.annotations.Test;

public class SearchV3ReadHoldPolicyTest {

  private static final String DATASET_V2 = "datasetindex_v2";
  private static final String CHART_V2 = "chartindex_v2";
  private static final String DATASET_V3 = "datasetindex_v3";
  private static final String CHART_V3 = "chartindex_v3";

  @Test
  public void testNoPairedV3IndexHoldsNothing() {
    // No V2 index exists: a fresh install has nothing to protect.
    assertTrue(SearchV3ReadHoldPolicy.incompleteV3Indices(Set.of(), Map.of()).isEmpty());
  }

  @Test
  public void testCompleteV3IndexLetsReadsMoveToV3() {
    assertTrue(
        SearchV3ReadHoldPolicy.incompleteV3Indices(
                Set.of(DATASET_V3), Map.of(DATASET_V3, IndexMigrationStamp.COMPLETE))
            .isEmpty());
  }

  @Test
  public void testIncompleteV3IndexIsReported() {
    assertEquals(
        SearchV3ReadHoldPolicy.incompleteV3Indices(
            Set.of(DATASET_V3), Map.of(DATASET_V3, IndexMigrationStamp.INCOMPLETE)),
        Set.of(DATASET_V3));
  }

  @Test
  public void testMissingOrUnmarkedV3IndexIsReported() {
    // A V3 index that does not exist, or exists without a mark, was not built by the migration.
    assertEquals(
        SearchV3ReadHoldPolicy.incompleteV3Indices(Set.of(DATASET_V3), Map.of()),
        Set.of(DATASET_V3));
  }

  @Test
  public void testOnlyTheIncompleteIndexOfSeveralIsReported() {
    Map<String, String> states = new HashMap<>();
    states.put(DATASET_V3, IndexMigrationStamp.COMPLETE);
    states.put(CHART_V3, IndexMigrationStamp.INCOMPLETE);

    assertEquals(
        SearchV3ReadHoldPolicy.incompleteV3Indices(Set.of(DATASET_V3, CHART_V3), states),
        Set.of(CHART_V3));
  }

  @Test
  public void testNoV2IndexLeavesReadsOnV3() {
    assertEquals(
        SearchV3ReadHoldPolicy.decide(Set.of(), Set.of(), Set.of(), Map.of(), Set.of()),
        SearchV3ReadHoldPolicy.Decision.LEAVE);
  }

  @Test
  public void testEveryCompletePairCommitsTheCutover() {
    assertEquals(
        SearchV3ReadHoldPolicy.decide(
            Set.of(DATASET_V2),
            Set.of(),
            Set.of(DATASET_V3),
            Map.of(DATASET_V3, IndexMigrationStamp.COMPLETE),
            Set.of()),
        SearchV3ReadHoldPolicy.Decision.COMMIT);
  }

  @Test
  public void testAnIncompletePairHoldsUntilTheCutoverIsRecorded() {
    assertEquals(
        SearchV3ReadHoldPolicy.decide(
            Set.of(DATASET_V2, CHART_V2),
            Set.of(DATASET_V2),
            Set.of(DATASET_V3, CHART_V3),
            Map.of(DATASET_V3, IndexMigrationStamp.COMPLETE),
            Set.of()),
        SearchV3ReadHoldPolicy.Decision.HOLD);
  }

  @Test
  public void testAnUnreadableIndexHoldsUntilTheCutoverIsRecorded() {
    assertEquals(
        SearchV3ReadHoldPolicy.decide(
            Set.of(DATASET_V2), Set.of(), Set.of(DATASET_V3), Map.of(), Set.of(DATASET_V3)),
        SearchV3ReadHoldPolicy.Decision.HOLD);
  }

  @Test
  public void testRetiredV2IndicesStayCutOverWhenTheV3MarkIsGone() {
    assertEquals(
        SearchV3ReadHoldPolicy.decide(
            Set.of(DATASET_V2), Set.of(DATASET_V2), Set.of(DATASET_V3), Map.of(), Set.of()),
        SearchV3ReadHoldPolicy.Decision.CUT_OVER);
  }

  @Test
  public void testEmptyIncompleteIndexAfterTheCutoverFailsStartup() {
    // The hold reports an incomplete index with no documents as unreadable.
    assertEquals(
        SearchV3ReadHoldPolicy.decide(
            Set.of(DATASET_V2),
            Set.of(DATASET_V2),
            Set.of(DATASET_V3),
            Map.of(DATASET_V3, IndexMigrationStamp.INCOMPLETE),
            Set.of(DATASET_V3)),
        SearchV3ReadHoldPolicy.Decision.FAIL);
  }

  @Test
  public void testIncompleteIndexWithDocumentsStaysCutOver() {
    assertEquals(
        SearchV3ReadHoldPolicy.decide(
            Set.of(DATASET_V2),
            Set.of(DATASET_V2),
            Set.of(DATASET_V3),
            Map.of(DATASET_V3, IndexMigrationStamp.INCOMPLETE),
            Set.of()),
        SearchV3ReadHoldPolicy.Decision.CUT_OVER);
  }

  @Test
  public void testIndexWithDocumentsAndNoMigrationMarkStaysCutOver() {
    assertEquals(
        SearchV3ReadHoldPolicy.decide(
            Set.of(DATASET_V2), Set.of(DATASET_V2), Set.of(DATASET_V3), Map.of(), Set.of()),
        SearchV3ReadHoldPolicy.Decision.CUT_OVER);
  }

  @Test
  public void testAnUnreadableIndexAfterTheCutoverFailsStartup() {
    assertEquals(
        SearchV3ReadHoldPolicy.decide(
            Set.of(DATASET_V2),
            Set.of(DATASET_V2),
            Set.of(DATASET_V3),
            Map.of(),
            Set.of(DATASET_V3)),
        SearchV3ReadHoldPolicy.Decision.FAIL);
  }
}
