package com.linkedin.metadata.utils;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.MetadataAttribution;
import com.linkedin.common.Owner;
import com.linkedin.common.OwnerArray;
import com.linkedin.common.Ownership;
import com.linkedin.common.OwnershipType;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.incident.IncidentAssignee;
import com.linkedin.incident.IncidentAssigneeArray;
import com.linkedin.incident.IncidentInfo;
import com.linkedin.incident.IncidentSource;
import com.linkedin.incident.IncidentSourceType;
import com.linkedin.incident.IncidentState;
import com.linkedin.incident.IncidentStatus;
import com.linkedin.incident.IncidentType;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
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

  @BeforeMethod
  public void tolerateUnknownDataByDefault() {
    UnknownDataHandlings.use(TolerantHandling.INSTANCE);
  }

  @AfterMethod
  public void resetUnknownDataHandling() {
    UnknownDataHandlings.reset();
  }

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
  public void testClearedRecordTheClientSawStaysCleared() {
    // Readers saw the source without its unknown urn, so clearing it was deliberate.
    final IncidentInfo request = incident();

    final UnknownReferenceMerger.MergeResult result =
        UnknownReferenceMerger.merge(request, incident().setSource(source(BOT)), registry);

    assertEquals(result.preserved(), 0);
    assertFalse(request.hasSource());
  }

  @Test
  public void testUnknownUrnOfARecordTheClientKeptIsKept() {
    final IncidentInfo request = incident().setSource(source(null));

    UnknownReferenceMerger.merge(request, incident().setSource(source(BOT)), registry);

    assertEquals(request.getSource().getSourceUrn(), BOT);
  }

  @Test
  public void testClearedRecordTheClientCouldNotSeeIsKept() {
    // An attribution whose required actor is of an unknown type is dropped whole on read.
    final Ownership stored = ownership(ALICE);
    stored.getOwners().get(0).setAttribution(new MetadataAttribution().setTime(0L).setActor(BOT));
    final Ownership request = ownership(ALICE);

    final UnknownReferenceMerger.MergeResult result =
        UnknownReferenceMerger.merge(request, stored, registry);

    assertEquals(result.preserved(), 1);
    assertEquals(request, stored);
  }

  @Test
  public void testEditedElementKeepsTheUnknownUrnsTheClientCouldNotSee() {
    final Ownership stored = ownership(ALICE);
    stored.getOwners().get(0).setAttribution(new MetadataAttribution().setTime(0L).setActor(BOT));
    final Ownership request = ownership(ALICE);
    request.getOwners().get(0).setType(OwnershipType.TECHNICAL_OWNER);

    final UnknownReferenceMerger.MergeResult result =
        UnknownReferenceMerger.merge(request, stored, registry);

    assertEquals(result.preserved(), 1);
    final Owner owner = request.getOwners().get(0);
    assertEquals(owner.getType(), OwnershipType.TECHNICAL_OWNER);
    assertEquals(owner.getAttribution().getActor(), BOT);
  }

  @Test
  public void testEditedElementIsNotMatchedWhenAnotherStoredElementHoldsTheSameUrns() {
    // Two stored owners hold only alice, so the edited one can't be told apart.
    final Ownership stored = ownership(ALICE, ALICE);
    stored.getOwners().get(0).setAttribution(new MetadataAttribution().setTime(0L).setActor(BOT));
    stored.getOwners().get(1).setType(OwnershipType.TECHNICAL_OWNER);
    final Ownership request = ownership(ALICE, ALICE);
    request.getOwners().get(0).setType(OwnershipType.BUSINESS_OWNER);
    request.getOwners().get(1).setType(OwnershipType.TECHNICAL_OWNER);

    UnknownReferenceMerger.merge(request, stored, registry);

    assertFalse(request.getOwners().get(0).hasAttribution());
    assertFalse(request.getOwners().get(1).hasAttribution());
  }

  @Test
  public void testClearedListOfElementsTheClientSawStaysAbsent() {
    final IncidentInfo request = incident();

    UnknownReferenceMerger.merge(
        request, incident().setAssignees(assignees(assignee(ALICE, BOT))), registry);

    assertFalse(request.hasAssignees());
  }

  @Test
  public void testClearedListKeepsOnlyElementsTheClientCouldNotSee() {
    final IncidentInfo request = incident();

    UnknownReferenceMerger.merge(
        request,
        incident().setAssignees(assignees(assignee(ALICE, BOT), assignee(BOT, null))),
        registry);

    assertEquals(request.getAssignees(), assignees(assignee(BOT, null)));
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
  public void testStrictHandlingMergesNothingAndNeverReadsTheStoredAspect() {
    // Reading the stored aspect parses the stored row; with tolerance off that cost isn't paid.
    final Ownership request = ownership(ALICE, OTHER_BOT);

    final UnknownReferenceMerger.MergeResult result =
        StrictHandling.INSTANCE.mergeStoredReferences(
            request,
            () -> {
              throw new AssertionError("stored aspect read with tolerance off");
            },
            registry);

    assertEquals(result, UnknownReferenceMerger.MergeResult.NONE);
    assertEquals(owners(request), List.of(ALICE, OTHER_BOT));
  }

  @Test
  public void testTolerantHandlingReadsTheStoredAspectOnce() {
    final Ownership request = ownership(ALICE);
    final AtomicInteger reads = new AtomicInteger();

    final UnknownReferenceMerger.MergeResult result =
        TolerantHandling.INSTANCE.mergeStoredReferences(
            request,
            () -> {
              reads.incrementAndGet();
              return ownership(ALICE, BOT);
            },
            registry);

    assertEquals(reads.get(), 1);
    assertEquals(result.preserved(), 1);
    assertEquals(owners(request), List.of(ALICE, BOT));
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

  private static IncidentInfo incident() {
    return new IncidentInfo()
        .setType(IncidentType.OPERATIONAL)
        .setEntities(new UrnArray(List.of(ALICE)))
        .setStatus(
            new IncidentStatus()
                .setState(IncidentState.ACTIVE)
                .setLastUpdated(new AuditStamp().setTime(0L).setActor(ALICE)))
        .setCreated(new AuditStamp().setTime(0L).setActor(ALICE));
  }

  private static IncidentSource source(final Urn sourceUrn) {
    final IncidentSource source = new IncidentSource().setType(IncidentSourceType.MANUAL);
    if (sourceUrn != null) {
      source.setSourceUrn(sourceUrn);
    }
    return source;
  }

  private static IncidentAssignee assignee(final Urn actor, final Urn impersonator) {
    final AuditStamp assignedAt = new AuditStamp().setTime(0L).setActor(ALICE);
    if (impersonator != null) {
      assignedAt.setImpersonator(impersonator);
    }
    return new IncidentAssignee().setActor(actor).setAssignedAt(assignedAt);
  }

  private static IncidentAssigneeArray assignees(final IncidentAssignee... assignees) {
    return new IncidentAssigneeArray(List.of(assignees));
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
