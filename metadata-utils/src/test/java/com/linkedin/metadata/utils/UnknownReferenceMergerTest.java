package com.linkedin.metadata.utils;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.Owner;
import com.linkedin.common.OwnerArray;
import com.linkedin.common.Ownership;
import com.linkedin.common.OwnershipType;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.testng.annotations.Test;

/**
 * A write keeps the stored references to entity types only a newer version knows, which reads drop,
 * and rejects ones it adds.
 */
public class UnknownReferenceMergerTest {

  private static final Urn ALICE = UrnUtils.getUrn("urn:li:corpuser:alice");
  private static final Urn CAROL = UrnUtils.getUrn("urn:li:corpuser:carol");
  private static final Urn BOT = UrnUtils.getUrn("urn:li:entityFromNewerBuild:bot1");
  private static final Urn OTHER_BOT = UrnUtils.getUrn("urn:li:entityFromNewerBuild:bot2");

  private final EntityRegistry registry = registry();

  @Test
  public void testWritingBackWhatWasReadKeepsTheStoredAspect() {
    final Ownership stored = ownership(ALICE, BOT);
    final Ownership request = ownership(ALICE);

    final UnknownReferenceMerger.MergeResult result =
        UnknownReferenceMerger.merge(request, stored, registry);

    assertEquals(result.preserved(), 1);
    assertEquals(request, stored);
  }

  @Test
  public void testKeptReferencesStayAtTheirStoredPosition() {
    final Ownership request = ownership(ALICE, CAROL);

    UnknownReferenceMerger.merge(request, ownership(ALICE, BOT), registry);

    assertEquals(owners(request), List.of(ALICE, BOT, CAROL));
  }

  @Test
  public void testClearingTheListKeepsWhatTheClientCouldNotSee() {
    final Ownership request = ownership();

    UnknownReferenceMerger.merge(request, ownership(ALICE, BOT), registry);

    assertEquals(owners(request), List.of(BOT));
  }

  @Test
  public void testAddedReferenceIsReportedAndNothingIsMerged() {
    final Ownership request = ownership(ALICE, OTHER_BOT);

    final UnknownReferenceMerger.MergeResult result =
        UnknownReferenceMerger.merge(request, ownership(ALICE, BOT), registry);

    assertEquals(result.added(), Set.of(OTHER_BOT.toString()));
    assertEquals(owners(request), List.of(ALICE, OTHER_BOT));
  }

  @Test
  public void testStoredReferenceSentBackIsNotAnAddition() {
    final Ownership request = ownership(BOT, ALICE);

    final UnknownReferenceMerger.MergeResult result =
        UnknownReferenceMerger.merge(request, ownership(ALICE, BOT), registry);

    assertTrue(result.added().isEmpty());
    assertEquals(owners(request), List.of(BOT, ALICE));
  }

  @Test
  public void testWithoutAStoredAspectEveryUnknownReferenceIsAnAddition() {
    final UnknownReferenceMerger.MergeResult result =
        UnknownReferenceMerger.merge(ownership(ALICE, BOT), null, registry);

    assertEquals(result.added(), Set.of(BOT.toString()));
  }

  @Test
  public void testStrictHandlingMergesNothing() {
    final Ownership request = ownership(ALICE, OTHER_BOT);

    final UnknownReferenceMerger.MergeResult result =
        StrictHandling.INSTANCE.mergeStoredReferences(request, ownership(ALICE, BOT), registry);

    assertEquals(result, UnknownReferenceMerger.MergeResult.NONE);
    assertEquals(owners(request), List.of(ALICE, OTHER_BOT));
  }

  private static Ownership ownership(final Urn... owners) {
    final OwnerArray ownerArray = new OwnerArray();
    for (Urn owner : owners) {
      ownerArray.add(new Owner().setOwner(owner).setType(OwnershipType.DATAOWNER));
    }
    return new Ownership()
        .setOwners(ownerArray)
        .setLastModified(new AuditStamp().setTime(0L).setActor(ALICE));
  }

  private static List<Urn> owners(final Ownership ownership) {
    return ownership.getOwners().stream().map(Owner::getOwner).toList();
  }

  private static EntityRegistry registry() {
    final EntityRegistry registry = mock(EntityRegistry.class);
    when(registry.findEntitySpec(anyString())).thenReturn(Optional.empty());
    when(registry.findEntitySpec("corpuser")).thenReturn(Optional.of(mock(EntitySpec.class)));
    return registry;
  }
}
