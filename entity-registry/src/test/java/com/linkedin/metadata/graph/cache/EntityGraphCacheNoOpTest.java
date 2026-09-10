package com.linkedin.metadata.graph.cache;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.testng.annotations.Test;

public class EntityGraphCacheNoOpTest {

  private static final EntityGraphCache NO_OP = EntityGraphCache.NO_OP;

  @Test
  public void expandReturnsDisabledMiss() {
    GraphReadResult result =
        NO_OP.expand(
            "domain",
            GraphSnapshotSource.SEARCH,
            TraversalDirection.FORWARD,
            List.of("urn:li:domain:root"),
            100,
            EntityGraphCache.USE_DEFINITION_MAX_DEPTH,
            ReadMode.CACHED);

    assertTrue(result.isMiss());
    assertEquals(((GraphReadResult.Miss) result).reason(), ReadMissReason.DISABLED);
  }

  @Test
  public void defaultExpandUsesCachedModeAndDefinitionMaxDepth() {
    GraphReadResult result =
        NO_OP.expand(
            "domain",
            GraphSnapshotSource.SEARCH,
            TraversalDirection.REVERSE,
            List.of("urn:li:domain:root"),
            50);

    assertTrue(result.isMiss());
  }

  @Test
  public void bindingsAndCandidatesAreEmpty() {
    assertEquals(NO_OP.bindingForKnownGraph(KnownEntityGraph.DOMAIN), Optional.empty());
    assertEquals(NO_OP.bindingForFilterField("domains.keyword"), Optional.empty());
    assertEquals(NO_OP.bindingForPolicyField("DOMAIN"), Optional.empty());
    assertEquals(NO_OP.getCandidateGraphIds("domain", "domainProperties"), Collections.emptySet());
  }

  @Test
  public void walkOrderedForwardAncestorsReturnsDisabledMiss() {
    AncestorWalkResult result =
        NO_OP.walkOrderedForwardAncestors(
            "domain", GraphSnapshotSource.SEARCH, "urn:li:domain:child", 5, ReadMode.CACHED);

    assertTrue(result.isMiss());
    assertEquals(((AncestorWalkResult.Miss) result).reason(), ReadMissReason.DISABLED);
  }

  @Test
  public void listRelatedReturnsDisabledMiss() {
    MembershipNeighborResult result =
        NO_OP.listRelated(
            "membership",
            GraphSnapshotSource.GRAPH,
            "urn:li:corpuser:alice",
            TraversalDirection.FORWARD,
            Set.of("IsMemberOfGroup"),
            1,
            0,
            10,
            ReadMode.CACHED);

    assertTrue(result.isMiss());
    assertEquals(((MembershipNeighborResult.Miss) result).reason(), ReadMissReason.DISABLED);
  }

  @Test
  public void invalidateOnSyncBatchIsNoOp() {
    NO_OP.invalidateOnSyncBatch(SyncGraphInvalidationBatch.empty());
  }
}
