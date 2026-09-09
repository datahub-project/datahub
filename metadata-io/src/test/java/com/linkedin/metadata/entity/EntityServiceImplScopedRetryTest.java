package com.linkedin.metadata.entity;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.plugins.validation.AspectValidationException;
import com.linkedin.util.Pair;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.testng.annotations.Test;

/**
 * Unit coverage for the pure decision logic of Stage 2 scoped retry that can be exercised without a
 * database. The end-to-end CAS convergence (a re-read inside the open transaction seeing the
 * winning writer's committed row, {@code isLatest} version adds converging to a single latest,
 * etc.) is a concurrency integration test against H2/MySQL/PostgreSQL that must be added separately
 * — it cannot be faithfully simulated here.
 */
public class EntityServiceImplScopedRetryTest {

  private static final Urn URN_A =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:mysql,a,PROD)");
  private static final Urn URN_B =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:mysql,b,PROD)");

  private static BatchItem itemFor(Urn urn) {
    BatchItem item = mock(BatchItem.class);
    when(item.getUrn()).thenReturn(urn);
    return item;
  }

  @Test
  public void recomputeKeepsAllAspectsOfConflictedUrnAndDropsOthers() {
    // URN_A has two aspects, URN_B has one. Recomputing only URN_A must carry BOTH of A's items
    // (per-URN atomicity) and none of B's.
    BatchItem a1 = itemFor(URN_A);
    BatchItem a2 = itemFor(URN_A);
    BatchItem b1 = itemFor(URN_B);

    List<BatchItem> filtered =
        EntityServiceImpl.filterItemsForRecompute(List.of(a1, a2, b1), Set.of(URN_A));

    assertEquals(filtered.size(), 2);
    assertTrue(filtered.contains(a1));
    assertTrue(filtered.contains(a2));
    assertTrue(filtered.stream().allMatch(i -> URN_A.equals(i.getUrn())));
  }

  @Test
  public void recomputeEmptyWhenNoUrnMatches() {
    assertTrue(
        EntityServiceImpl.filterItemsForRecompute(List.of(itemFor(URN_A)), Set.of(URN_B))
            .isEmpty());
  }

  private static final String ASPECT_KEY = "datasetKey";
  private static final String ASPECT_STATUS = "status";

  private static ChangeMCP changeFor(Urn urn, String aspect) {
    ChangeMCP item = mock(ChangeMCP.class);
    when(item.getUrn()).thenReturn(urn);
    when(item.getAspectName()).thenReturn(aspect);
    return item;
  }

  @Test
  public void committedKeysOfExtractsOnlyCommittedUrnAspectPairs() {
    // Only COMMITTED (urn, aspect) pairs seed the cross-pass double-commit guard; CONFLICT and NOOP
    // must be excluded (a conflicted aspect still needs to be retried, a no-op was never written).
    BatchWriteResult pass =
        new BatchWriteResult(
            List.of(
                AspectWriteResult.committed(URN_A, ASPECT_KEY, 0L),
                AspectWriteResult.conflict(URN_A, ASPECT_STATUS),
                AspectWriteResult.noop(URN_B, ASPECT_STATUS)));

    assertEquals(EntityServiceImpl.committedKeysOf(pass), Set.of(Pair.of(URN_A, ASPECT_KEY)));
  }

  @Test
  public void committedSiblingSkippedOnRetryWhileConflictedSiblingRetried() {
    // Models the double-commit guard: URN_A had datasetKey COMMITTED and status CONFLICT in pass 0.
    // On the scoped retry the sub-batch is scoped by URN (so it re-includes BOTH of URN_A's
    // aspects),
    // but the already-committed datasetKey must be skipped while the conflicted status is
    // re-persisted.
    Set<Pair<Urn, String>> committedKeys = Set.of(Pair.of(URN_A, ASPECT_KEY));

    assertTrue(
        EntityServiceImpl.isAlreadyCommitted(committedKeys, changeFor(URN_A, ASPECT_KEY)),
        "committed sibling must be skipped (no double-commit)");
    assertTrue(
        !EntityServiceImpl.isAlreadyCommitted(committedKeys, changeFor(URN_A, ASPECT_STATUS)),
        "conflicted sibling on the same URN must still be retried");
  }

  private static Pair<ChangeMCP, Set<AspectValidationException>> failureFor(
      Urn urn, String aspect) {
    return Pair.of(changeFor(urn, aspect), Set.<AspectValidationException>of());
  }

  @Test
  public void failedKeysOfExtractsUrnAspectPairs() {
    List<Pair<ChangeMCP, Set<AspectValidationException>>> failures =
        List.of(failureFor(URN_A, ASPECT_STATUS), failureFor(URN_B, ASPECT_KEY));

    assertEquals(
        EntityServiceImpl.failedKeysOf(failures),
        Set.of(Pair.of(URN_A, ASPECT_STATUS), Pair.of(URN_B, ASPECT_KEY)));
  }

  @Test
  public void appendNewFailedResultsRecordsEachUrnAspectOnceAcrossPasses() {
    // A terminally validation-failing aspect (URN_A/status) is re-included in every URN-scoped
    // scoped-retry sub-batch and re-fails validation on each pass. It must be recorded exactly once
    // — not once per pass — while a genuinely new failure on a later pass is still appended.
    List<Pair<ChangeMCP, Set<AspectValidationException>>> accumulator = new ArrayList<>();
    accumulator.add(failureFor(URN_A, ASPECT_STATUS)); // pass-0 failure
    Set<Pair<Urn, String>> seenFailedKeys =
        new HashSet<>(EntityServiceImpl.failedKeysOf(accumulator));

    // Retry pass re-reports the same (URN_A, status) failure plus a new (URN_B, status) one.
    EntityServiceImpl.appendNewFailedResults(
        accumulator,
        List.of(failureFor(URN_A, ASPECT_STATUS), failureFor(URN_B, ASPECT_STATUS)),
        seenFailedKeys);

    assertEquals(accumulator.size(), 2, "repeated (urn, aspect) recorded once; new one appended");
    assertEquals(
        EntityServiceImpl.failedKeysOf(accumulator),
        Set.of(Pair.of(URN_A, ASPECT_STATUS), Pair.of(URN_B, ASPECT_STATUS)));

    // A third pass that only re-reports already-seen failures adds nothing.
    EntityServiceImpl.appendNewFailedResults(
        accumulator,
        List.of(failureFor(URN_A, ASPECT_STATUS), failureFor(URN_B, ASPECT_STATUS)),
        seenFailedKeys);
    assertEquals(accumulator.size(), 2, "no new keys on a repeat pass → no growth");
  }

  @Test
  public void appendNewFailedResultsKeepsDistinctSameKeyFailuresWithinAPassButNotAcrossPasses() {
    // Two DISTINCT items sharing (URN_A, status) that both fail in the SAME pass must both be kept
    // — the dedup only removes a (urn, aspect) that already failed in an EARLIER pass, so it never
    // collapses two distinct failures reported together (preserving the un-deduped pass-0
    // behavior).
    List<Pair<ChangeMCP, Set<AspectValidationException>>> accumulator = new ArrayList<>();
    Set<Pair<Urn, String>> seenFailedKeys = new HashSet<>();

    EntityServiceImpl.appendNewFailedResults(
        accumulator,
        List.of(failureFor(URN_A, ASPECT_STATUS), failureFor(URN_A, ASPECT_STATUS)),
        seenFailedKeys);
    assertEquals(accumulator.size(), 2, "distinct same-key failures within one pass are both kept");

    // A later pass restating (URN_A, status) adds nothing — it already failed in an earlier pass.
    EntityServiceImpl.appendNewFailedResults(
        accumulator, List.of(failureFor(URN_A, ASPECT_STATUS)), seenFailedKeys);
    assertEquals(accumulator.size(), 2, "cross-pass repeat of an earlier-failed key is suppressed");
  }

  @Test
  public void branchScopedRecomputeMapsDerivedConflictToParentBase() {
    // A derives B (VersionSet -> isLatest patch on the previous latest). A conflict on the derived
    // B recomputes A (its parent base); re-running A re-derives B. Not just B, and not all bases.
    Map<Pair<Urn, String>, Set<Urn>> derivedToParents =
        Map.of(Pair.of(URN_B, "versionProperties"), Set.of(URN_A));
    BatchWriteResult result =
        new BatchWriteResult(List.of(AspectWriteResult.conflict(URN_B, "versionProperties")));

    assertEquals(EntityServiceImpl.branchScopedRecompute(result, derivedToParents), Set.of(URN_A));
  }

  @Test
  public void branchScopedRecomputeIsJustTheUrnWithNoDerivations() {
    // A plain base URN with no in-transaction side effects is absent from the map → recompute only
    // itself (the no-side-effect common case reduces to exactly the conflicted base URN).
    BatchWriteResult result =
        new BatchWriteResult(List.of(AspectWriteResult.conflict(URN_A, ASPECT_STATUS)));

    assertEquals(EntityServiceImpl.branchScopedRecompute(result, Map.of()), Set.of(URN_A));
  }

  @Test
  public void writeGateKeysAreOnePerUrnAspect() {
    // The write gate keys on the (urn, aspect) conflict unit: cross-aspect writers on the same URN
    // get DISTINCT keys (they don't serialize), while two writers of the SAME (urn, aspect) share a
    // key (they do serialize).
    Map<String, Set<String>> urnAspects =
        Map.of(
            URN_A.toString(), Set.of("status", "ownership"),
            URN_B.toString(), Set.of("status"));

    List<String> keys = EntityServiceImpl.writeGateKeys(urnAspects);

    assertEquals(keys.size(), 3);
    assertTrue(keys.contains(EntityServiceImpl.writeGateKey(URN_A.toString(), "status")));
    assertTrue(keys.contains(EntityServiceImpl.writeGateKey(URN_A.toString(), "ownership")));
    assertTrue(keys.contains(EntityServiceImpl.writeGateKey(URN_B.toString(), "status")));
    // same URN, different aspect → different key (no cross-aspect over-serialization)
    assertTrue(
        !EntityServiceImpl.writeGateKey(URN_A.toString(), "status")
            .equals(EntityServiceImpl.writeGateKey(URN_A.toString(), "ownership")));
    // same (urn, aspect) → identical key (concurrent writers of it serialize)
    assertEquals(
        EntityServiceImpl.writeGateKey(URN_A.toString(), "status"),
        EntityServiceImpl.writeGateKey(URN_A.toString(), "status"));
  }
}
