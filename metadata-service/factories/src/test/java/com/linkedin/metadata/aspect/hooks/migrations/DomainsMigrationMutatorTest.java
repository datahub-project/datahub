package com.linkedin.metadata.aspect.hooks.migrations;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.domain.DomainAssociation;
import com.linkedin.domain.DomainAssociationArray;
import com.linkedin.domain.Domains;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.models.registry.EntityRegistry;
import java.util.List;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class DomainsMigrationMutatorTest {

  private static final Urn DOMAIN_A = UrnUtils.getUrn("urn:li:domain:a");
  private static final Urn DOMAIN_B = UrnUtils.getUrn("urn:li:domain:b");

  private RetrieverContext retrieverContext;

  @BeforeMethod
  public void setUp() {
    EntityRegistry entityRegistry = mock(EntityRegistry.class);
    AspectRetriever mockAspectRetriever = mock(AspectRetriever.class);
    when(mockAspectRetriever.getEntityRegistry()).thenReturn(entityRegistry);
    retrieverContext = mock(RetrieverContext.class);
    when(retrieverContext.getAspectRetriever()).thenReturn(mockAspectRetriever);
  }

  @Test
  public void testTransform_backfillsAssociationsFromDomains() {
    DomainsMigrationMutator mutator = new DomainsMigrationMutator();

    Domains original = makeDomains(DOMAIN_A, DOMAIN_B);

    RecordTemplate result = mutator.transform(original, retrieverContext);

    assertNotNull(result);
    Domains migrated = new Domains(result.data());
    assertTrue(migrated.hasDomainAssociations());
    DomainAssociationArray assocs = migrated.getDomainAssociations();
    assertEquals(assocs.size(), 2);
    assertEquals(assocs.get(0).getDomain(), DOMAIN_A);
    assertEquals(assocs.get(1).getDomain(), DOMAIN_B);
    // Empty attribution
    assertTrue(!assocs.get(0).hasAttribution());
  }

  @Test
  public void testTransform_emptyDomains_returnsCopy() {
    DomainsMigrationMutator mutator = new DomainsMigrationMutator();

    Domains original = makeDomains();

    RecordTemplate result = mutator.transform(original, retrieverContext);

    // Still returns a copy so version gets bumped
    assertNotNull(result);
    Domains migrated = new Domains(result.data());
    assertTrue(!migrated.hasDomainAssociations() || migrated.getDomainAssociations().isEmpty());
  }

  @Test
  public void testTransform_associationsAlreadyPresent_keepsThem() {
    DomainsMigrationMutator mutator = new DomainsMigrationMutator();

    DomainAssociation existingAssoc = new DomainAssociation();
    existingAssoc.setDomain(DOMAIN_A);
    existingAssoc.setContext("user-set");

    Domains original = makeDomains(DOMAIN_A);
    original.setDomainAssociations(new DomainAssociationArray(List.of(existingAssoc)));

    RecordTemplate result = mutator.transform(original, retrieverContext);

    assertNotNull(result);
    Domains migrated = new Domains(result.data());
    assertEquals(migrated.getDomainAssociations().size(), 1);
    assertEquals(migrated.getDomainAssociations().get(0).getContext(), "user-set");
  }

  @Test
  public void testTransform_doesNotMutateOriginal() {
    DomainsMigrationMutator mutator = new DomainsMigrationMutator();

    Domains original = makeDomains(DOMAIN_A);

    mutator.transform(original, retrieverContext);

    // Original should not have domainAssociations set
    assertTrue(!original.hasDomainAssociations());
  }

  @Test
  public void testAspectNameAndVersions() {
    DomainsMigrationMutator mutator = new DomainsMigrationMutator();
    assertEquals(mutator.getAspectName(), "domains");
    assertEquals(mutator.getSourceVersion(), 1L);
    assertEquals(mutator.getTargetVersion(), 2L);
  }

  private static Domains makeDomains(Urn... urns) {
    Domains domains = new Domains();
    domains.setDomains(new UrnArray(List.of(urns)));
    return domains;
  }
}
