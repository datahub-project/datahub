package com.linkedin.metadata.timeline.eventgenerator;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.Owner;
import com.linkedin.common.OwnerArray;
import com.linkedin.common.Ownership;
import com.linkedin.common.OwnershipType;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.timeline.data.ChangeCategory;
import com.linkedin.metadata.timeline.data.ChangeEvent;
import com.linkedin.metadata.timeline.data.ChangeOperation;
import com.linkedin.metadata.timeline.data.entity.OwnerChangeEvent;
import com.linkedin.mxe.SystemMetadata;
import java.net.URISyntaxException;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

public class OwnershipChangeEventGeneratorTest extends AbstractTestNGSpringContextTests {

  private static final String TEST_ENTITY_URN =
      "urn:li:dataset:(urn:li:dataPlatform:hive,SampleTable,PROD)";
  private static final String TEST_OWNER_URN_1 = "urn:li:corpuser:user1";
  private static final String TEST_OWNER_URN_2 = "urn:li:corpuser:user2";
  private static final String TEST_OWNER_URN_3 = "urn:li:corpuser:user3";
  private static final String TEST_OWNER_GROUP_URN_1 = "urn:li:corpGroup:group1";
  private static final String TEST_OWNERSHIP_TYPE_URN_1 =
      "urn:li:ownershipType:__system__technical_owner";
  private static final String TEST_OWNERSHIP_TYPE_URN_2 =
      "urn:li:ownershipType:__system__data_steward";

  private static Urn getTestUrn() throws URISyntaxException {
    return Urn.createFromString(TEST_ENTITY_URN);
  }

  private static AuditStamp getTestAuditStamp() throws URISyntaxException {
    return new AuditStamp()
        .setActor(Urn.createFromString("urn:li:corpuser:__datahub_system"))
        .setTime(1683829509553L);
  }

  private static Aspect<Ownership> createOwnershipAspect(List<Owner> owners) {
    Ownership ownership = new Ownership();
    ownership.setOwners(new OwnerArray(owners));
    return new Aspect<>(ownership, new SystemMetadata());
  }

  private static Owner createOwner(String ownerUrn, OwnershipType type, String typeUrn)
      throws URISyntaxException {
    Owner owner = new Owner();
    owner.setOwner(Urn.createFromString(ownerUrn));
    owner.setType(type);
    if (typeUrn != null) {
      owner.setTypeUrn(Urn.createFromString(typeUrn));
    }
    return owner;
  }

  private static void validateOwnerChangeEvent(
      ChangeEvent changeEvent,
      ChangeOperation expectedOperation,
      String expectedOwnerUrn,
      OwnershipType expectedType,
      String expectedTypeUrn) {
    assertNotNull(changeEvent);
    assertEquals(changeEvent.getClass(), OwnerChangeEvent.class);
    assertEquals(changeEvent.getOperation(), expectedOperation);
    assertEquals(changeEvent.getEntityUrn(), TEST_ENTITY_URN);
    assertEquals(changeEvent.getCategory(), ChangeCategory.OWNERSHIP);

    assertNotNull(changeEvent.getParameters());
    assertEquals(changeEvent.getParameters().get("ownerUrn"), expectedOwnerUrn);
    assertEquals(changeEvent.getParameters().get("ownerType"), expectedType.toString());
    if (expectedTypeUrn != null) {
      assertNotNull(changeEvent.getParameters().get("ownerTypeUrn"));
      assertEquals(changeEvent.getParameters().get("ownerTypeUrn"), expectedTypeUrn);
    }
  }

  @Test
  public void testAddOwnerWithSameOwnershipType() throws Exception {
    OwnershipChangeEventGenerator generator = new OwnershipChangeEventGenerator();

    Urn urn = getTestUrn();
    String entity = "dataset";
    String aspect = "ownership";
    AuditStamp auditStamp = getTestAuditStamp();

    Aspect<Ownership> from = createOwnershipAspect(List.of());
    Aspect<Ownership> to =
        createOwnershipAspect(
            List.of(createOwner(TEST_OWNER_URN_1, OwnershipType.TECHNICAL_OWNER, null)));

    List<ChangeEvent> actual = generator.getChangeEvents(urn, entity, aspect, from, to, auditStamp);

    assertEquals(actual.size(), 1);
    validateOwnerChangeEvent(
        actual.get(0), ChangeOperation.ADD, TEST_OWNER_URN_1, OwnershipType.TECHNICAL_OWNER, null);
  }

  @Test
  public void testRemoveOwnerWithSameOwnershipType() throws Exception {
    OwnershipChangeEventGenerator generator = new OwnershipChangeEventGenerator();

    Urn urn = getTestUrn();
    String entity = "dataset";
    String aspect = "ownership";
    AuditStamp auditStamp = getTestAuditStamp();

    Aspect<Ownership> from =
        createOwnershipAspect(
            List.of(createOwner(TEST_OWNER_URN_1, OwnershipType.TECHNICAL_OWNER, null)));
    Aspect<Ownership> to = createOwnershipAspect(List.of());

    List<ChangeEvent> actual = generator.getChangeEvents(urn, entity, aspect, from, to, auditStamp);

    assertEquals(actual.size(), 1);
    validateOwnerChangeEvent(
        actual.get(0),
        ChangeOperation.REMOVE,
        TEST_OWNER_URN_1,
        OwnershipType.TECHNICAL_OWNER,
        null);
  }

  @Test
  public void testAddOwnerWithTypeUrn() throws Exception {
    OwnershipChangeEventGenerator generator = new OwnershipChangeEventGenerator();

    Urn urn = getTestUrn();
    String entity = "dataset";
    String aspect = "ownership";
    AuditStamp auditStamp = getTestAuditStamp();

    Aspect<Ownership> from = createOwnershipAspect(List.of());
    Aspect<Ownership> to =
        createOwnershipAspect(
            List.of(
                createOwner(TEST_OWNER_URN_1, OwnershipType.CUSTOM, TEST_OWNERSHIP_TYPE_URN_1)));

    List<ChangeEvent> actual = generator.getChangeEvents(urn, entity, aspect, from, to, auditStamp);

    assertEquals(actual.size(), 1);
    validateOwnerChangeEvent(
        actual.get(0),
        ChangeOperation.ADD,
        TEST_OWNER_URN_1,
        OwnershipType.CUSTOM,
        TEST_OWNERSHIP_TYPE_URN_1);
  }

  @Test
  public void testMultipleOwnersWithDifferentOwnershipTypes() throws Exception {
    OwnershipChangeEventGenerator generator = new OwnershipChangeEventGenerator();

    Urn urn = getTestUrn();
    String entity = "dataset";
    String aspect = "ownership";
    AuditStamp auditStamp = getTestAuditStamp();

    Aspect<Ownership> from =
        createOwnershipAspect(
            List.of(createOwner(TEST_OWNER_URN_1, OwnershipType.TECHNICAL_OWNER, null)));

    Aspect<Ownership> to =
        createOwnershipAspect(
            List.of(
                createOwner(TEST_OWNER_URN_2, OwnershipType.TECHNICAL_OWNER, null),
                createOwner(TEST_OWNER_URN_3, OwnershipType.DATA_STEWARD, null)));

    List<ChangeEvent> actual = generator.getChangeEvents(urn, entity, aspect, from, to, auditStamp);

    assertEquals(actual.size(), 3);

    List<ChangeEvent> sortedEvents =
        actual.stream()
            .sorted(
                Comparator.comparing(ChangeEvent::getOperation)
                    .thenComparing(e -> (String) e.getParameters().get("ownerUrn")))
            .collect(Collectors.toList());

    validateOwnerChangeEvent(
        sortedEvents.get(0),
        ChangeOperation.ADD,
        TEST_OWNER_URN_2,
        OwnershipType.TECHNICAL_OWNER,
        null);
    validateOwnerChangeEvent(
        sortedEvents.get(1),
        ChangeOperation.ADD,
        TEST_OWNER_URN_3,
        OwnershipType.DATA_STEWARD,
        null);
    validateOwnerChangeEvent(
        sortedEvents.get(2),
        ChangeOperation.REMOVE,
        TEST_OWNER_URN_1,
        OwnershipType.TECHNICAL_OWNER,
        null);
  }

  @Test
  public void testMultipleOwnersPerOwnershipType() throws Exception {
    OwnershipChangeEventGenerator generator = new OwnershipChangeEventGenerator();

    Urn urn = getTestUrn();
    String entity = "dataset";
    String aspect = "ownership";
    AuditStamp auditStamp = getTestAuditStamp();

    Aspect<Ownership> from =
        createOwnershipAspect(
            List.of(createOwner(TEST_OWNER_URN_1, OwnershipType.TECHNICAL_OWNER, null)));

    Aspect<Ownership> to =
        createOwnershipAspect(
            List.of(
                createOwner(TEST_OWNER_URN_1, OwnershipType.TECHNICAL_OWNER, null),
                createOwner(TEST_OWNER_URN_2, OwnershipType.TECHNICAL_OWNER, null),
                createOwner(TEST_OWNER_URN_3, OwnershipType.TECHNICAL_OWNER, null)));

    List<ChangeEvent> actual = generator.getChangeEvents(urn, entity, aspect, from, to, auditStamp);

    assertEquals(actual.size(), 2);

    List<ChangeEvent> sortedEvents =
        actual.stream()
            .sorted(Comparator.comparing(e -> (String) e.getParameters().get("ownerUrn")))
            .collect(Collectors.toList());

    validateOwnerChangeEvent(
        sortedEvents.get(0),
        ChangeOperation.ADD,
        TEST_OWNER_URN_2,
        OwnershipType.TECHNICAL_OWNER,
        null);
    validateOwnerChangeEvent(
        sortedEvents.get(1),
        ChangeOperation.ADD,
        TEST_OWNER_URN_3,
        OwnershipType.TECHNICAL_OWNER,
        null);
  }

  @Test
  public void testNoChanges() throws Exception {
    OwnershipChangeEventGenerator generator = new OwnershipChangeEventGenerator();

    Urn urn = getTestUrn();
    String entity = "dataset";
    String aspect = "ownership";
    AuditStamp auditStamp = getTestAuditStamp();

    Aspect<Ownership> from =
        createOwnershipAspect(
            List.of(createOwner(TEST_OWNER_URN_1, OwnershipType.TECHNICAL_OWNER, null)));

    Aspect<Ownership> to =
        createOwnershipAspect(
            List.of(createOwner(TEST_OWNER_URN_1, OwnershipType.TECHNICAL_OWNER, null)));

    List<ChangeEvent> actual = generator.getChangeEvents(urn, entity, aspect, from, to, auditStamp);

    assertEquals(actual.size(), 0);
  }

  @Test
  public void testDifferentOwnerTypesSameOwner() throws Exception {
    OwnershipChangeEventGenerator generator = new OwnershipChangeEventGenerator();

    Urn urn = getTestUrn();
    String entity = "dataset";
    String aspect = "ownership";
    AuditStamp auditStamp = getTestAuditStamp();

    Aspect<Ownership> from =
        createOwnershipAspect(
            List.of(createOwner(TEST_OWNER_URN_1, OwnershipType.TECHNICAL_OWNER, null)));

    Aspect<Ownership> to =
        createOwnershipAspect(
            List.of(createOwner(TEST_OWNER_URN_1, OwnershipType.DATA_STEWARD, null)));

    List<ChangeEvent> actual = generator.getChangeEvents(urn, entity, aspect, from, to, auditStamp);

    assertEquals(actual.size(), 2);

    List<ChangeEvent> sortedEvents =
        actual.stream()
            .sorted(Comparator.comparing(ChangeEvent::getOperation))
            .collect(Collectors.toList());

    validateOwnerChangeEvent(
        sortedEvents.get(0),
        ChangeOperation.ADD,
        TEST_OWNER_URN_1,
        OwnershipType.DATA_STEWARD,
        null);
    validateOwnerChangeEvent(
        sortedEvents.get(1),
        ChangeOperation.REMOVE,
        TEST_OWNER_URN_1,
        OwnershipType.TECHNICAL_OWNER,
        null);
  }

  @Test
  public void testCustomOwnershipTypesWithDifferentTypeUrns() throws Exception {
    OwnershipChangeEventGenerator generator = new OwnershipChangeEventGenerator();

    Urn urn = getTestUrn();
    String entity = "dataset";
    String aspect = "ownership";
    AuditStamp auditStamp = getTestAuditStamp();

    Aspect<Ownership> from =
        createOwnershipAspect(
            List.of(
                createOwner(TEST_OWNER_URN_1, OwnershipType.CUSTOM, TEST_OWNERSHIP_TYPE_URN_1)));

    Aspect<Ownership> to =
        createOwnershipAspect(
            List.of(
                createOwner(TEST_OWNER_URN_1, OwnershipType.CUSTOM, TEST_OWNERSHIP_TYPE_URN_1),
                createOwner(TEST_OWNER_URN_1, OwnershipType.CUSTOM, TEST_OWNERSHIP_TYPE_URN_2)));

    List<ChangeEvent> actual = generator.getChangeEvents(urn, entity, aspect, from, to, auditStamp);

    assertEquals(actual.size(), 1);
    validateOwnerChangeEvent(
        actual.get(0),
        ChangeOperation.ADD,
        TEST_OWNER_URN_1,
        OwnershipType.CUSTOM,
        TEST_OWNERSHIP_TYPE_URN_2);
  }

  @Test
  public void testGroupOwners() throws Exception {
    OwnershipChangeEventGenerator generator = new OwnershipChangeEventGenerator();

    Urn urn = getTestUrn();
    String entity = "dataset";
    String aspect = "ownership";
    AuditStamp auditStamp = getTestAuditStamp();

    Aspect<Ownership> from = createOwnershipAspect(List.of());

    Aspect<Ownership> to =
        createOwnershipAspect(
            List.of(createOwner(TEST_OWNER_GROUP_URN_1, OwnershipType.TECHNICAL_OWNER, null)));

    List<ChangeEvent> actual = generator.getChangeEvents(urn, entity, aspect, from, to, auditStamp);

    assertEquals(actual.size(), 1);
    validateOwnerChangeEvent(
        actual.get(0),
        ChangeOperation.ADD,
        TEST_OWNER_GROUP_URN_1,
        OwnershipType.TECHNICAL_OWNER,
        null);
  }

  @Test
  public void testComplexScenarioMultipleTypesAndOwners() throws Exception {
    OwnershipChangeEventGenerator generator = new OwnershipChangeEventGenerator();

    Urn urn = getTestUrn();
    String entity = "dataset";
    String aspect = "ownership";
    AuditStamp auditStamp = getTestAuditStamp();

    Aspect<Ownership> from =
        createOwnershipAspect(
            List.of(
                createOwner(TEST_OWNER_URN_1, OwnershipType.TECHNICAL_OWNER, null),
                createOwner(TEST_OWNER_URN_2, OwnershipType.TECHNICAL_OWNER, null),
                createOwner(TEST_OWNER_URN_3, OwnershipType.DATA_STEWARD, null)));

    Aspect<Ownership> to =
        createOwnershipAspect(
            List.of(
                createOwner(TEST_OWNER_URN_1, OwnershipType.TECHNICAL_OWNER, null),
                createOwner(TEST_OWNER_URN_3, OwnershipType.TECHNICAL_OWNER, null),
                createOwner(
                    TEST_OWNER_GROUP_URN_1, OwnershipType.CUSTOM, TEST_OWNERSHIP_TYPE_URN_1)));

    List<ChangeEvent> actual = generator.getChangeEvents(urn, entity, aspect, from, to, auditStamp);

    assertEquals(actual.size(), 4);

    Set<ChangeOperation> operations =
        actual.stream().map(ChangeEvent::getOperation).collect(Collectors.toSet());
    assertEquals(operations, Set.of(ChangeOperation.ADD, ChangeOperation.REMOVE));

    long addCount = actual.stream().filter(e -> e.getOperation() == ChangeOperation.ADD).count();
    long removeCount =
        actual.stream().filter(e -> e.getOperation() == ChangeOperation.REMOVE).count();

    assertEquals(addCount, 2);
    assertEquals(removeCount, 2);
  }

  @Test
  public void testFromEmptyToMultipleOwners() throws Exception {
    OwnershipChangeEventGenerator generator = new OwnershipChangeEventGenerator();

    Urn urn = getTestUrn();
    String entity = "dataset";
    String aspect = "ownership";
    AuditStamp auditStamp = getTestAuditStamp();

    Aspect<Ownership> from = createOwnershipAspect(List.of());

    Aspect<Ownership> to =
        createOwnershipAspect(
            List.of(
                createOwner(TEST_OWNER_URN_1, OwnershipType.TECHNICAL_OWNER, null),
                createOwner(TEST_OWNER_URN_2, OwnershipType.DATA_STEWARD, null),
                createOwner(TEST_OWNER_URN_3, OwnershipType.CUSTOM, TEST_OWNERSHIP_TYPE_URN_1)));

    List<ChangeEvent> actual = generator.getChangeEvents(urn, entity, aspect, from, to, auditStamp);

    assertEquals(actual.size(), 3);

    Set<ChangeOperation> operations =
        actual.stream().map(ChangeEvent::getOperation).collect(Collectors.toSet());
    assertEquals(operations, Set.of(ChangeOperation.ADD));

    Set<String> actualOwnerUrns =
        actual.stream()
            .map(e -> (String) e.getParameters().get("ownerUrn"))
            .collect(Collectors.toSet());
    assertEquals(actualOwnerUrns, Set.of(TEST_OWNER_URN_1, TEST_OWNER_URN_2, TEST_OWNER_URN_3));
  }

  @Test
  public void testFromMultipleOwnersToEmpty() throws Exception {
    OwnershipChangeEventGenerator generator = new OwnershipChangeEventGenerator();

    Urn urn = getTestUrn();
    String entity = "dataset";
    String aspect = "ownership";
    AuditStamp auditStamp = getTestAuditStamp();

    Aspect<Ownership> from =
        createOwnershipAspect(
            List.of(
                createOwner(TEST_OWNER_URN_1, OwnershipType.TECHNICAL_OWNER, null),
                createOwner(TEST_OWNER_URN_2, OwnershipType.DATA_STEWARD, null),
                createOwner(TEST_OWNER_URN_3, OwnershipType.CUSTOM, TEST_OWNERSHIP_TYPE_URN_1)));

    Aspect<Ownership> to = createOwnershipAspect(List.of());

    List<ChangeEvent> actual = generator.getChangeEvents(urn, entity, aspect, from, to, auditStamp);

    assertEquals(actual.size(), 3);

    Set<ChangeOperation> operations =
        actual.stream().map(ChangeEvent::getOperation).collect(Collectors.toSet());
    assertEquals(operations, Set.of(ChangeOperation.REMOVE));

    Set<String> actualOwnerUrns =
        actual.stream()
            .map(e -> (String) e.getParameters().get("ownerUrn"))
            .collect(Collectors.toSet());
    assertEquals(actualOwnerUrns, Set.of(TEST_OWNER_URN_1, TEST_OWNER_URN_2, TEST_OWNER_URN_3));
  }

  @Test
  public void testChangeEventDescriptionFormat() throws Exception {
    OwnershipChangeEventGenerator generator = new OwnershipChangeEventGenerator();

    Urn urn = getTestUrn();
    String entity = "dataset";
    String aspect = "ownership";
    AuditStamp auditStamp = getTestAuditStamp();

    Aspect<Ownership> from = createOwnershipAspect(List.of());

    Aspect<Ownership> to =
        createOwnershipAspect(
            List.of(createOwner(TEST_OWNER_URN_1, OwnershipType.TECHNICAL_OWNER, null)));

    List<ChangeEvent> actual = generator.getChangeEvents(urn, entity, aspect, from, to, auditStamp);

    assertEquals(actual.size(), 1);
    ChangeEvent changeEvent = actual.get(0);
    assertNotNull(changeEvent.getDescription());
    assertEquals(
        changeEvent.getDescription(),
        "'user1' added as a `TECHNICAL_OWNER` of '" + TEST_ENTITY_URN + "'.");
  }
}
