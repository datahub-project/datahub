package com.linkedin.metadata.graph.cache.snapshot;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNull;

import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringMap;
import com.linkedin.metadata.search.SearchEntity;
import java.util.List;
import org.testng.annotations.Test;

public class EntityGraphSnapshotBuilderUtilitiesTest {

  @Test
  public void extractRelatedUrnReadsDirectExtraField() {
    SearchEntity entity =
        new SearchEntity()
            .setEntity(UrnUtils.getUrn("urn:li:domain:child"))
            .setExtraFields(new StringMap(java.util.Map.of("parentDomain", "urn:li:domain:root")));

    assertEquals(
        EntityGraphSnapshotBuilder.extractRelatedUrn(entity, "parentDomain"), "urn:li:domain:root");
  }

  @Test
  public void extractRelatedUrnReadsLeafAndKeywordFields() {
    SearchEntity entity =
        new SearchEntity()
            .setEntity(UrnUtils.getUrn("urn:li:domain:child"))
            .setExtraFields(
                new StringMap(java.util.Map.of("parentDomain.keyword", "urn:li:domain:root")));

    assertEquals(
        EntityGraphSnapshotBuilder.extractRelatedUrn(entity, "domains.parentDomain"),
        "urn:li:domain:root");
  }

  @Test
  public void extractRelatedUrnStripsQuotesAndReturnsNullWhenMissing() {
    SearchEntity entity =
        new SearchEntity()
            .setEntity(UrnUtils.getUrn("urn:li:domain:child"))
            .setExtraFields(
                new StringMap(java.util.Map.of("parentDomain", "\"urn:li:domain:root\"")));

    assertEquals(
        EntityGraphSnapshotBuilder.extractRelatedUrn(entity, "parentDomain"), "urn:li:domain:root");
    assertNull(EntityGraphSnapshotBuilder.extractRelatedUrn(entity, null));
    assertNull(
        EntityGraphSnapshotBuilder.extractRelatedUrn(
            new SearchEntity().setEntity(UrnUtils.getUrn("urn:li:domain:child")), "parentDomain"));
  }

  @Test
  public void topologyFingerprintIsStableAndOrderIndependent() {
    List<EntityGraphSnapshot.DirectedEdge> edgesA =
        List.of(
            EntityGraphSnapshot.DirectedEdge.builder()
                .sourceUrn("urn:li:domain:b")
                .destinationUrn("urn:li:domain:a")
                .relationshipType("IsPartOf")
                .build(),
            EntityGraphSnapshot.DirectedEdge.builder()
                .sourceUrn("urn:li:domain:c")
                .destinationUrn("urn:li:domain:b")
                .relationshipType("IsPartOf")
                .build());
    List<EntityGraphSnapshot.DirectedEdge> edgesB = List.of(edgesA.get(1), edgesA.get(0));

    String fpA = EntityGraphSnapshotBuilder.topologyFingerprint(edgesA);
    String fpB = EntityGraphSnapshotBuilder.topologyFingerprint(edgesB);

    assertEquals(fpA.length(), 32);
    assertEquals(fpA, fpB);
    assertNotEquals(fpA, EntityGraphSnapshotBuilder.topologyFingerprint(List.of()));
  }
}
