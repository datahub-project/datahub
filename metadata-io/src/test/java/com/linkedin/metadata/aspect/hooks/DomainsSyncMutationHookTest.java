package com.linkedin.metadata.aspect.hooks;

import static com.linkedin.metadata.Constants.DOMAINS_ASPECT_NAME;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.datahub.context.OperationFingerprint;
import com.linkedin.common.MetadataAttribution;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringMap;
import com.linkedin.domain.DomainAssociation;
import com.linkedin.domain.DomainAssociationArray;
import com.linkedin.domain.Domains;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.entity.validation.ValidationException;
import com.linkedin.util.Pair;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class DomainsSyncMutationHookTest {

  private static final Urn TEST_ENTITY_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:postgres,test.table,PROD)");
  private static final Urn DOMAIN_A = UrnUtils.getUrn("urn:li:domain:a");
  private static final Urn DOMAIN_B = UrnUtils.getUrn("urn:li:domain:b");
  private static final Urn DOMAIN_C = UrnUtils.getUrn("urn:li:domain:c");

  private static final AspectPluginConfig HOOK_CONFIG =
      AspectPluginConfig.builder()
          .className(DomainsSyncMutationHook.class.getName())
          .enabled(true)
          .supportedOperations(List.of("UPSERT"))
          .supportedEntityAspectNames(
              List.of(
                  AspectPluginConfig.EntityAspectName.builder()
                      .entityName("*")
                      .aspectName(DOMAINS_ASPECT_NAME)
                      .build()))
          .build();

  private RetrieverContext retrieverContext;

  @BeforeMethod
  public void setup() {
    retrieverContext = mock(RetrieverContext.class);
  }

  // ── Legacy path: only domains provided ─────────────────────────────────────

  @Test
  public void testLegacyWrite_newEntity_createsAssociations() {
    Domains proposed = makeDomains(DOMAIN_A, DOMAIN_B);
    ChangeMCP item = makeChangeMCP(proposed, null);

    List<Pair<ChangeMCP, Boolean>> result = applyHook(item);

    assertEquals(result.size(), 1);
    assertTrue(result.get(0).getSecond());
    assertTrue(proposed.hasDomainAssociations());
    DomainAssociationArray assocs = proposed.getDomainAssociations();
    assertEquals(assocs.size(), 2);
    assertEquals(assocs.get(0).getDomain(), DOMAIN_A);
    assertEquals(assocs.get(1).getDomain(), DOMAIN_B);
    assertFalse(assocs.get(0).hasAttribution());
  }

  @Test
  public void testLegacyWrite_addDomain_preservesExistingAttribution() {
    MetadataAttribution attr = makeAttribution();
    DomainAssociation existingAssoc = new DomainAssociation();
    existingAssoc.setDomain(DOMAIN_A);
    existingAssoc.setAttribution(attr);

    Domains previous = makeDomains(DOMAIN_A);
    previous.setDomainAssociations(new DomainAssociationArray(List.of(existingAssoc)));

    Domains proposed = makeDomains(DOMAIN_A, DOMAIN_B);
    // No domainAssociations on proposed → legacy path
    ChangeMCP item = makeChangeMCP(proposed, previous);

    applyHook(item);

    DomainAssociationArray assocs = proposed.getDomainAssociations();
    assertEquals(assocs.size(), 2);
    // Existing attribution for DOMAIN_A is preserved
    assertEquals(assocs.get(0).getDomain(), DOMAIN_A);
    assertTrue(assocs.get(0).hasAttribution());
    assertEquals(assocs.get(0).getAttribution(), attr);
    // New domain B gets empty attribution
    assertEquals(assocs.get(1).getDomain(), DOMAIN_B);
    assertFalse(assocs.get(1).hasAttribution());
  }

  @Test
  public void testLegacyWrite_removeDomain_removesAllAssociations() {
    DomainAssociation assocA = new DomainAssociation();
    assocA.setDomain(DOMAIN_A);
    DomainAssociation assocB = new DomainAssociation();
    assocB.setDomain(DOMAIN_B);
    // Propagated association for B
    DomainAssociation assocBPropagated = new DomainAssociation();
    assocBPropagated.setDomain(DOMAIN_B);
    assocBPropagated.setAttribution(makePropagatedAttribution());

    Domains previous = makeDomains(DOMAIN_A, DOMAIN_B);
    previous.setDomainAssociations(
        new DomainAssociationArray(List.of(assocA, assocB, assocBPropagated)));

    // Remove DOMAIN_B
    Domains proposed = makeDomains(DOMAIN_A);
    ChangeMCP item = makeChangeMCP(proposed, previous);

    applyHook(item);

    DomainAssociationArray assocs = proposed.getDomainAssociations();
    assertEquals(assocs.size(), 1);
    assertEquals(assocs.get(0).getDomain(), DOMAIN_A);
  }

  // ── New path: domainAssociations provided ──────────────────────────────────

  @Test
  public void testNewPathWrite_derivesDomainsFromAssociations() {
    DomainAssociation assocA = new DomainAssociation();
    assocA.setDomain(DOMAIN_A);
    DomainAssociation assocB = new DomainAssociation();
    assocB.setDomain(DOMAIN_B);

    Domains proposed = new Domains();
    // Domains set consistently with associations (well-behaved new-path client)
    proposed.setDomains(new UrnArray(List.of(DOMAIN_A, DOMAIN_B)));
    proposed.setDomainAssociations(new DomainAssociationArray(List.of(assocA, assocB)));

    ChangeMCP item = makeChangeMCP(proposed, null);

    applyHook(item);

    // domains derived from non-propagated associations
    assertEquals(proposed.getDomains().size(), 2);
    assertTrue(proposed.getDomains().contains(DOMAIN_A));
    assertTrue(proposed.getDomains().contains(DOMAIN_B));
  }

  @Test
  public void testNewPathWrite_propagatedEntriesIncludedInDomainsAfterManual() {
    DomainAssociation assocA = new DomainAssociation();
    assocA.setDomain(DOMAIN_A);

    DomainAssociation propagatedB = new DomainAssociation();
    propagatedB.setDomain(DOMAIN_B);
    propagatedB.setAttribution(makePropagatedAttribution());

    Domains proposed = new Domains();
    proposed.setDomains(new UrnArray(List.of(DOMAIN_A, DOMAIN_B)));
    proposed.setDomainAssociations(new DomainAssociationArray(List.of(assocA, propagatedB)));

    ChangeMCP item = makeChangeMCP(proposed, null);

    applyHook(item);

    // All URNs appear in domains, manual first then propagated
    assertEquals(proposed.getDomains().size(), 2);
    assertEquals(proposed.getDomains().get(0), DOMAIN_A);
    assertEquals(proposed.getDomains().get(1), DOMAIN_B);
    // Both associations are preserved
    assertEquals(proposed.getDomainAssociations().size(), 2);
  }

  @Test
  public void testNewPathWrite_deduplicatesDomains() {
    // Two non-propagated associations for the same domain
    DomainAssociation assocA1 = new DomainAssociation();
    assocA1.setDomain(DOMAIN_A);
    DomainAssociation assocA2 = new DomainAssociation();
    assocA2.setDomain(DOMAIN_A);
    assocA2.setAttribution(makeAttribution());

    Domains proposed = new Domains();
    proposed.setDomains(new UrnArray(List.of(DOMAIN_A)));
    proposed.setDomainAssociations(new DomainAssociationArray(List.of(assocA1, assocA2)));

    ChangeMCP item = makeChangeMCP(proposed, null);

    applyHook(item);

    // Deduplicated in domains
    assertEquals(proposed.getDomains().size(), 1);
    assertEquals(proposed.getDomains().get(0), DOMAIN_A);
  }

  // ── Conflict detection ─────────────────────────────────────────────────────

  @Test
  public void testBothFieldsInSync_allowed() {
    // domains and associations are consistent → no error
    DomainAssociation assocA = new DomainAssociation();
    assocA.setDomain(DOMAIN_A);

    Domains proposed = new Domains();
    proposed.setDomains(new UrnArray(List.of(DOMAIN_A)));
    proposed.setDomainAssociations(new DomainAssociationArray(List.of(assocA)));

    Domains previous = makeDomains(DOMAIN_B);
    ChangeMCP item = makeChangeMCP(proposed, previous);

    // Should not throw
    List<Pair<ChangeMCP, Boolean>> result = applyHook(item);
    assertTrue(result.get(0).getSecond());
  }

  @Test
  public void testBothFieldsInSyncWithPropagated_allowed() {
    // All URNs (including propagated) match domains → allowed
    DomainAssociation assocA = new DomainAssociation();
    assocA.setDomain(DOMAIN_A);
    DomainAssociation propagatedB = new DomainAssociation();
    propagatedB.setDomain(DOMAIN_B);
    propagatedB.setAttribution(makePropagatedAttribution());

    Domains proposed = new Domains();
    proposed.setDomains(new UrnArray(List.of(DOMAIN_A, DOMAIN_B)));
    proposed.setDomainAssociations(new DomainAssociationArray(List.of(assocA, propagatedB)));

    Domains previous = makeDomains(DOMAIN_C);
    ChangeMCP item = makeChangeMCP(proposed, previous);

    // Should not throw despite both fields changing from previous
    List<Pair<ChangeMCP, Boolean>> result = applyHook(item);
    assertTrue(result.get(0).getSecond());
  }

  @Test(expectedExceptions = ValidationException.class)
  public void testBothFieldsInconsistentAndDomainsChanged_throws() {
    DomainAssociation assocB = new DomainAssociation();
    assocB.setDomain(DOMAIN_B);

    Domains proposed = new Domains();
    proposed.setDomains(new UrnArray(List.of(DOMAIN_A))); // inconsistent with associations
    proposed.setDomainAssociations(new DomainAssociationArray(List.of(assocB)));

    Domains previous = makeDomains(DOMAIN_C); // domains changed from previous
    ChangeMCP item = makeChangeMCP(proposed, previous);

    applyHook(item);
  }

  @Test
  public void testBothFieldsInconsistentButDomainsUnchanged_allowed() {
    // Associations differ from domains but domains didn't change from previous
    // → associations win (caller updated only associations)
    DomainAssociation assocB = new DomainAssociation();
    assocB.setDomain(DOMAIN_B);

    Domains proposed = new Domains();
    proposed.setDomains(new UrnArray(List.of(DOMAIN_A))); // same as previous
    proposed.setDomainAssociations(new DomainAssociationArray(List.of(assocB)));

    Domains previous = makeDomains(DOMAIN_A); // domains unchanged
    ChangeMCP item = makeChangeMCP(proposed, previous);

    applyHook(item);

    // Domains derived from associations
    assertEquals(proposed.getDomains().size(), 1);
    assertEquals(proposed.getDomains().get(0), DOMAIN_B);
  }

  // ── Read-modify-write scenarios ───────────────────────────────────────────

  @Test
  public void testReadModifyWrite_onlyDomainsChanged_syncsAssociations() {
    // Previous has both fields in sync (as if written by the hook previously)
    DomainAssociation assocA = new DomainAssociation();
    assocA.setDomain(DOMAIN_A);

    Domains previous = makeDomains(DOMAIN_A);
    previous.setDomainAssociations(new DomainAssociationArray(List.of(assocA)));

    // Read-modify-write: caller read the full aspect, changed only domains, left associations stale
    Domains proposed = new Domains();
    proposed.setDomains(new UrnArray(List.of(DOMAIN_A, DOMAIN_B)));
    proposed.setDomainAssociations(new DomainAssociationArray(List.of(assocA))); // stale from read

    ChangeMCP item = makeChangeMCP(proposed, previous);

    applyHook(item);

    // Associations should be derived from domains (not left stale)
    DomainAssociationArray assocs = proposed.getDomainAssociations();
    assertEquals(assocs.size(), 2);
    Set<Urn> assocUrns =
        assocs.stream().map(DomainAssociation::getDomain).collect(Collectors.toSet());
    assertEquals(assocUrns, Set.of(DOMAIN_A, DOMAIN_B));
  }

  @Test
  public void testReadModifyWrite_onlyAssociationsChanged_syncsDomains() {
    // Previous has both fields in sync
    DomainAssociation assocA = new DomainAssociation();
    assocA.setDomain(DOMAIN_A);

    Domains previous = makeDomains(DOMAIN_A);
    previous.setDomainAssociations(new DomainAssociationArray(List.of(assocA)));

    // Read-modify-write: caller changed only associations, left domains stale
    DomainAssociation newAssocB = new DomainAssociation();
    newAssocB.setDomain(DOMAIN_B);

    Domains proposed = new Domains();
    proposed.setDomains(new UrnArray(List.of(DOMAIN_A))); // stale from read
    proposed.setDomainAssociations(new DomainAssociationArray(List.of(assocA, newAssocB)));

    ChangeMCP item = makeChangeMCP(proposed, previous);

    applyHook(item);

    // Domains should be derived from associations
    assertEquals(proposed.getDomains().size(), 2);
    assertTrue(proposed.getDomains().contains(DOMAIN_A));
    assertTrue(proposed.getDomains().contains(DOMAIN_B));
  }

  // ── Edge cases ─────────────────────────────────────────────────────────────

  @Test
  public void testNullProposedAspect_noOp() {
    ChangeMCP item = mock(ChangeMCP.class);
    when(item.getAspectName()).thenReturn(DOMAINS_ASPECT_NAME);
    when(item.getUrn()).thenReturn(TEST_ENTITY_URN);
    when(item.getAspect(Domains.class)).thenReturn(null);

    List<Pair<ChangeMCP, Boolean>> result = applyHook(item);
    assertFalse(result.get(0).getSecond());
  }

  @Test
  public void testNonDomainsAspect_passThrough() {
    ChangeMCP item = mock(ChangeMCP.class);
    when(item.getAspectName()).thenReturn("glossaryTermInfo");

    List<Pair<ChangeMCP, Boolean>> result = applyHook(item);
    assertFalse(result.get(0).getSecond());
  }

  @Test
  public void testEmptyDomainsAndEmptyAssociations() {
    Domains proposed = new Domains();
    proposed.setDomains(new UrnArray());
    proposed.setDomainAssociations(new DomainAssociationArray());

    ChangeMCP item = makeChangeMCP(proposed, null);

    applyHook(item);

    assertEquals(proposed.getDomains().size(), 0);
    assertEquals(proposed.getDomainAssociations().size(), 0);
  }

  @Test
  public void testLegacyWrite_noPreviousAssociations_createsFromScratch() {
    // Previous aspect exists but has no domainAssociations (v1 data)
    Domains previous = makeDomains(DOMAIN_A);
    // No domainAssociations on previous

    Domains proposed = makeDomains(DOMAIN_A, DOMAIN_B);
    ChangeMCP item = makeChangeMCP(proposed, previous);

    applyHook(item);

    DomainAssociationArray assocs = proposed.getDomainAssociations();
    assertEquals(assocs.size(), 2);
    Set<Urn> assocUrns =
        assocs.stream().map(DomainAssociation::getDomain).collect(Collectors.toSet());
    assertEquals(assocUrns, Set.of(DOMAIN_A, DOMAIN_B));
  }

  // ── Ordering: manual before propagated ───────────────────────────────────

  @Test
  public void testNewPath_manualBeforePropagatedInAssociations() {
    // Propagated entry listed first in input; after sync it should come last
    DomainAssociation propagatedA = new DomainAssociation();
    propagatedA.setDomain(DOMAIN_A);
    propagatedA.setAttribution(makePropagatedAttribution());

    DomainAssociation manualB = new DomainAssociation();
    manualB.setDomain(DOMAIN_B);

    DomainAssociation manualC = new DomainAssociation();
    manualC.setDomain(DOMAIN_C);

    Domains proposed = new Domains();
    proposed.setDomains(new UrnArray(List.of(DOMAIN_A, DOMAIN_B, DOMAIN_C)));
    proposed.setDomainAssociations(
        new DomainAssociationArray(List.of(propagatedA, manualB, manualC)));

    ChangeMCP item = makeChangeMCP(proposed, null);

    applyHook(item);

    DomainAssociationArray assocs = proposed.getDomainAssociations();
    assertEquals(assocs.size(), 3);
    // Manual entries first
    assertEquals(assocs.get(0).getDomain(), DOMAIN_B);
    assertFalse(DomainsSyncMutationHook.isPropagated(assocs.get(0)));
    assertEquals(assocs.get(1).getDomain(), DOMAIN_C);
    assertFalse(DomainsSyncMutationHook.isPropagated(assocs.get(1)));
    // Propagated entry last
    assertEquals(assocs.get(2).getDomain(), DOMAIN_A);
    assertTrue(DomainsSyncMutationHook.isPropagated(assocs.get(2)));

    // domains reordered: manual first, then propagated
    assertEquals(proposed.getDomains().size(), 3);
    assertEquals(proposed.getDomains().get(0), DOMAIN_B);
    assertEquals(proposed.getDomains().get(1), DOMAIN_C);
    assertEquals(proposed.getDomains().get(2), DOMAIN_A);
  }

  @Test
  public void testLegacyPath_preservedPropagatedAssociationsMovedToEnd() {
    // Previous has: manual A, propagated B, manual C (B is propagated but in the middle)
    DomainAssociation assocA = new DomainAssociation();
    assocA.setDomain(DOMAIN_A);
    DomainAssociation propagatedB = new DomainAssociation();
    propagatedB.setDomain(DOMAIN_B);
    propagatedB.setAttribution(makePropagatedAttribution());
    DomainAssociation assocC = new DomainAssociation();
    assocC.setDomain(DOMAIN_C);

    Domains previous = makeDomains(DOMAIN_A, DOMAIN_B, DOMAIN_C);
    previous.setDomainAssociations(
        new DomainAssociationArray(List.of(assocA, propagatedB, assocC)));

    // Legacy write keeps all three domains
    Domains proposed = makeDomains(DOMAIN_A, DOMAIN_B, DOMAIN_C);
    ChangeMCP item = makeChangeMCP(proposed, previous);

    applyHook(item);

    DomainAssociationArray assocs = proposed.getDomainAssociations();
    assertEquals(assocs.size(), 3);
    // Manual entries first (A, C), then propagated (B)
    assertFalse(DomainsSyncMutationHook.isPropagated(assocs.get(0)));
    assertFalse(DomainsSyncMutationHook.isPropagated(assocs.get(1)));
    assertTrue(DomainsSyncMutationHook.isPropagated(assocs.get(2)));
    assertEquals(assocs.get(2).getDomain(), DOMAIN_B);
  }

  @Test
  public void testAllManual_orderUnchanged() {
    DomainAssociation assocA = new DomainAssociation();
    assocA.setDomain(DOMAIN_A);
    DomainAssociation assocB = new DomainAssociation();
    assocB.setDomain(DOMAIN_B);

    Domains proposed = new Domains();
    proposed.setDomains(new UrnArray(List.of(DOMAIN_A, DOMAIN_B)));
    proposed.setDomainAssociations(new DomainAssociationArray(List.of(assocA, assocB)));

    ChangeMCP item = makeChangeMCP(proposed, null);

    applyHook(item);

    DomainAssociationArray assocs = proposed.getDomainAssociations();
    assertEquals(assocs.size(), 2);
    assertEquals(assocs.get(0).getDomain(), DOMAIN_A);
    assertEquals(assocs.get(1).getDomain(), DOMAIN_B);
  }

  @Test
  public void testAllPropagated_orderUnchanged() {
    DomainAssociation propagatedA = new DomainAssociation();
    propagatedA.setDomain(DOMAIN_A);
    propagatedA.setAttribution(makePropagatedAttribution());

    DomainAssociation propagatedB = new DomainAssociation();
    propagatedB.setDomain(DOMAIN_B);
    propagatedB.setAttribution(makePropagatedAttribution());

    Domains proposed = new Domains();
    proposed.setDomains(new UrnArray(List.of(DOMAIN_A, DOMAIN_B)));
    proposed.setDomainAssociations(new DomainAssociationArray(List.of(propagatedA, propagatedB)));

    ChangeMCP item = makeChangeMCP(proposed, null);

    applyHook(item);

    DomainAssociationArray assocs = proposed.getDomainAssociations();
    assertEquals(assocs.size(), 2);
    assertEquals(assocs.get(0).getDomain(), DOMAIN_A);
    assertEquals(assocs.get(1).getDomain(), DOMAIN_B);
    // domains includes all URNs (propagated included)
    assertEquals(proposed.getDomains().size(), 2);
    assertEquals(proposed.getDomains().get(0), DOMAIN_A);
    assertEquals(proposed.getDomains().get(1), DOMAIN_B);
  }

  // ── Helpers ────────────────────────────────────────────────────────────────

  private List<Pair<ChangeMCP, Boolean>> applyHook(ChangeMCP item) {
    DomainsSyncMutationHook hook = new DomainsSyncMutationHook();
    hook.setConfig(HOOK_CONFIG);
    return hook.writeMutation(OperationFingerprint.EMPTY, List.of(item), retrieverContext).toList();
  }

  private static Domains makeDomains(Urn... urns) {
    Domains domains = new Domains();
    domains.setDomains(new UrnArray(List.of(urns)));
    return domains;
  }

  private static ChangeMCP makeChangeMCP(Domains proposed, Domains previous) {
    ChangeMCP item = mock(ChangeMCP.class);
    when(item.getAspectName()).thenReturn(DOMAINS_ASPECT_NAME);
    when(item.getUrn()).thenReturn(TEST_ENTITY_URN);
    when(item.getAspect(Domains.class)).thenReturn(proposed);
    when(item.getPreviousAspect(Domains.class)).thenReturn(previous);
    return item;
  }

  private static MetadataAttribution makeAttribution() {
    MetadataAttribution attr = new MetadataAttribution();
    attr.setTime(System.currentTimeMillis());
    attr.setActor(UrnUtils.getUrn("urn:li:corpuser:testUser"));
    return attr;
  }

  private static MetadataAttribution makePropagatedAttribution() {
    MetadataAttribution attr = makeAttribution();
    attr.setSourceDetail(new StringMap(Map.of("propagated", "true")));
    return attr;
  }
}
