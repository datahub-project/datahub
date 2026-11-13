package com.linkedin.metadata.timeline.eventgenerator;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.domain.DomainAssociation;
import com.linkedin.domain.DomainAssociationArray;
import com.linkedin.domain.Domains;
import com.linkedin.metadata.timeline.data.ChangeEvent;
import com.linkedin.metadata.timeline.data.ChangeOperation;
import com.linkedin.metadata.timeline.data.entity.DomainChangeEvent;
import com.linkedin.mxe.SystemMetadata;
import java.net.URISyntaxException;
import java.util.List;
import org.testng.annotations.Test;

public class SingleDomainChangeEventGeneratorTest {

  private static final String TEST_ENTITY_URN =
      "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.table,PROD)";
  private static final String DOMAIN_URN_1 = "urn:li:domain:engineering";
  private static final String DOMAIN_URN_2 = "urn:li:domain:marketing";
  private static final String TEST_CONTEXT =
      "{\"origin\":\"urn:li:dataset:(urn:li:dataPlatform:hive,OriginTable,PROD)\",\"propagated\":\"true\",\"actor\":\"urn:li:corpuser:test\"}";

  private static Urn getTestUrn() throws URISyntaxException {
    return Urn.createFromString(TEST_ENTITY_URN);
  }

  private static AuditStamp getTestAuditStamp() throws URISyntaxException {
    return new AuditStamp()
        .setActor(Urn.createFromString("urn:li:corpuser:__datahub_system"))
        .setTime(1683829509553L);
  }

  private static Aspect<Domains> createDomainsAspect(String domainUrn, String context)
      throws URISyntaxException {
    Domains domains = new Domains();
    if (domainUrn != null) {
      Urn urn = Urn.createFromString(domainUrn);
      domains.setDomains(new UrnArray(urn));
      if (context != null) {
        DomainAssociation association = new DomainAssociation();
        association.setDomain(urn);
        association.setContext(context);
        domains.setDomainAssociations(new DomainAssociationArray(association));
      }
    } else {
      domains.setDomains(new UrnArray());
    }
    return new Aspect<>(domains, new SystemMetadata());
  }

  private static void validateChangeEvent(
      ChangeEvent changeEvent,
      ChangeOperation expectedOperation,
      String expectedDomainUrn,
      String expectedContext) {
    assertNotNull(changeEvent);
    assertEquals(changeEvent.getOperation(), expectedOperation);
    assertEquals(changeEvent.getModifier(), expectedDomainUrn);
    assertEquals(changeEvent.getEntityUrn(), TEST_ENTITY_URN);

    assertNotNull(changeEvent.getParameters());
    assertEquals(changeEvent.getParameters().get("domainUrn"), expectedDomainUrn);
    assertEquals(
        changeEvent.getParameters().get("context"),
        expectedContext != null ? expectedContext : "{}");
  }

  @Test
  public void testAddDomainWithContext() throws Exception {
    SingleDomainChangeEventGenerator generator = new SingleDomainChangeEventGenerator();

    Aspect<Domains> from = createDomainsAspect(null, null);
    Aspect<Domains> to = createDomainsAspect(DOMAIN_URN_1, TEST_CONTEXT);

    List<ChangeEvent> actual =
        generator.getChangeEvents(
            getTestUrn(), "dataset", "domains", from, to, getTestAuditStamp());

    assertEquals(actual.size(), 1);
    validateChangeEvent(actual.get(0), ChangeOperation.ADD, DOMAIN_URN_1, TEST_CONTEXT);
  }

  @Test
  public void testAddDomainWithoutContext() throws Exception {
    SingleDomainChangeEventGenerator generator = new SingleDomainChangeEventGenerator();

    Aspect<Domains> from = createDomainsAspect(null, null);
    Aspect<Domains> to = createDomainsAspect(DOMAIN_URN_1, null);

    List<ChangeEvent> actual =
        generator.getChangeEvents(
            getTestUrn(), "dataset", "domains", from, to, getTestAuditStamp());

    assertEquals(actual.size(), 1);
    validateChangeEvent(actual.get(0), ChangeOperation.ADD, DOMAIN_URN_1, null);
  }

  @Test
  public void testRemoveDomainWithContext() throws Exception {
    SingleDomainChangeEventGenerator generator = new SingleDomainChangeEventGenerator();

    Aspect<Domains> from = createDomainsAspect(DOMAIN_URN_1, TEST_CONTEXT);
    Aspect<Domains> to = createDomainsAspect(null, null);

    List<ChangeEvent> actual =
        generator.getChangeEvents(
            getTestUrn(), "dataset", "domains", from, to, getTestAuditStamp());

    assertEquals(actual.size(), 1);
    validateChangeEvent(actual.get(0), ChangeOperation.REMOVE, DOMAIN_URN_1, TEST_CONTEXT);
  }

  @Test
  public void testRemoveDomainWithoutContext() throws Exception {
    SingleDomainChangeEventGenerator generator = new SingleDomainChangeEventGenerator();

    Aspect<Domains> from = createDomainsAspect(DOMAIN_URN_1, null);
    Aspect<Domains> to = createDomainsAspect(null, null);

    List<ChangeEvent> actual =
        generator.getChangeEvents(
            getTestUrn(), "dataset", "domains", from, to, getTestAuditStamp());

    assertEquals(actual.size(), 1);
    validateChangeEvent(actual.get(0), ChangeOperation.REMOVE, DOMAIN_URN_1, null);
  }

  @Test
  public void testDomainChanged() throws Exception {
    SingleDomainChangeEventGenerator generator = new SingleDomainChangeEventGenerator();

    Aspect<Domains> from = createDomainsAspect(DOMAIN_URN_1, TEST_CONTEXT);
    Aspect<Domains> to = createDomainsAspect(DOMAIN_URN_2, null);

    List<ChangeEvent> actual =
        generator.getChangeEvents(
            getTestUrn(), "dataset", "domains", from, to, getTestAuditStamp());

    assertEquals(actual.size(), 2);

    ChangeEvent removeEvent =
        actual.stream()
            .filter(e -> e.getOperation() == ChangeOperation.REMOVE)
            .findFirst()
            .orElse(null);
    ChangeEvent addEvent =
        actual.stream()
            .filter(e -> e.getOperation() == ChangeOperation.ADD)
            .findFirst()
            .orElse(null);

    validateChangeEvent(removeEvent, ChangeOperation.REMOVE, DOMAIN_URN_1, TEST_CONTEXT);
    validateChangeEvent(addEvent, ChangeOperation.ADD, DOMAIN_URN_2, null);
  }

  @Test
  public void testNoChanges() throws Exception {
    SingleDomainChangeEventGenerator generator = new SingleDomainChangeEventGenerator();

    Aspect<Domains> from = createDomainsAspect(DOMAIN_URN_1, TEST_CONTEXT);
    Aspect<Domains> to = createDomainsAspect(DOMAIN_URN_1, TEST_CONTEXT);

    List<ChangeEvent> actual =
        generator.getChangeEvents(
            getTestUrn(), "dataset", "domains", from, to, getTestAuditStamp());

    assertEquals(actual.size(), 0);
  }

  @Test
  public void testChangeEventIsDomainChangeEvent() throws Exception {
    SingleDomainChangeEventGenerator generator = new SingleDomainChangeEventGenerator();

    Aspect<Domains> from = createDomainsAspect(null, null);
    Aspect<Domains> to = createDomainsAspect(DOMAIN_URN_1, TEST_CONTEXT);

    List<ChangeEvent> actual =
        generator.getChangeEvents(
            getTestUrn(), "dataset", "domains", from, to, getTestAuditStamp());

    assertEquals(actual.size(), 1);

    ChangeEvent changeEvent = actual.get(0);
    assertEquals(changeEvent.getClass(), DomainChangeEvent.class);

    DomainChangeEvent domainChangeEvent = (DomainChangeEvent) changeEvent;
    assertNotNull(domainChangeEvent.getParameters());
    assertEquals(domainChangeEvent.getParameters().get("domainUrn"), DOMAIN_URN_1);
    assertEquals(domainChangeEvent.getParameters().get("context"), TEST_CONTEXT);
  }

  @Test
  public void testDescriptionContainsDomainAndEntity() throws Exception {
    SingleDomainChangeEventGenerator generator = new SingleDomainChangeEventGenerator();

    Aspect<Domains> from = createDomainsAspect(null, null);
    Aspect<Domains> to = createDomainsAspect(DOMAIN_URN_1, null);

    List<ChangeEvent> actual =
        generator.getChangeEvents(
            getTestUrn(), "dataset", "domains", from, to, getTestAuditStamp());

    assertEquals(actual.size(), 1);
    assertNotNull(actual.get(0).getDescription());
    assertTrue(actual.get(0).getDescription().contains("engineering"));
    assertTrue(actual.get(0).getDescription().contains(TEST_ENTITY_URN));
  }
}
