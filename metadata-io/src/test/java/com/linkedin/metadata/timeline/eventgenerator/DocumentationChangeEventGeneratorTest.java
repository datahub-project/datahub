package com.linkedin.metadata.timeline.eventgenerator;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.Documentation;
import com.linkedin.common.DocumentationAssociation;
import com.linkedin.common.DocumentationAssociationArray;
import com.linkedin.common.MetadataAttribution;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.timeline.data.ChangeCategory;
import com.linkedin.metadata.timeline.data.ChangeEvent;
import com.linkedin.metadata.timeline.data.ChangeOperation;
import com.linkedin.mxe.SystemMetadata;
import java.net.URISyntaxException;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

public class DocumentationChangeEventGeneratorTest extends AbstractTestNGSpringContextTests {

  private static final String TEST_ENTITY_URN =
      "urn:li:dataset:(urn:li:dataPlatform:hive,SampleTable,PROD)";
  private static final String TEST_ACTOR_URN_1 = "urn:li:corpuser:user1";
  private static final String TEST_ACTOR_URN_2 = "urn:li:corpuser:user2";
  private static final String TEST_SOURCE_URN_1 = "urn:li:dataHubAction:action1";
  private static final String TEST_SOURCE_URN_2 = "urn:li:dataHubAction:action2";

  private static Urn getTestUrn() throws URISyntaxException {
    return Urn.createFromString(TEST_ENTITY_URN);
  }

  private static AuditStamp getTestAuditStamp() throws URISyntaxException {
    return new AuditStamp()
        .setActor(Urn.createFromString("urn:li:corpuser:__datahub_system"))
        .setTime(1683829509553L);
  }

  private static Aspect<Documentation> createDocumentationAspect(
      List<DocumentationAssociation> associations) {
    Documentation documentation = new Documentation();
    documentation.setDocumentations(new DocumentationAssociationArray(associations));
    return new Aspect<>(documentation, new SystemMetadata());
  }

  private static DocumentationAssociation createAssociationWithSource(
      String docText, String actorUrn, String sourceUrn) throws URISyntaxException {
    DocumentationAssociation association = new DocumentationAssociation();
    association.setDocumentation(docText);
    MetadataAttribution attribution = new MetadataAttribution();
    attribution.setActor(Urn.createFromString(actorUrn));
    attribution.setTime(1683829509553L);
    if (sourceUrn != null) {
      attribution.setSource(Urn.createFromString(sourceUrn));
    }
    association.setAttribution(attribution);
    return association;
  }

  private static DocumentationAssociation createAssociationNoAttribution(String docText) {
    DocumentationAssociation association = new DocumentationAssociation();
    association.setDocumentation(docText);
    return association;
  }

  private static void validateChangeEvent(
      ChangeEvent changeEvent,
      ChangeOperation expectedOperation,
      String expectedDoc,
      String expectedModifier) {
    assertNotNull(changeEvent);
    assertEquals(changeEvent.getOperation(), expectedOperation);
    assertEquals(changeEvent.getCategory(), ChangeCategory.DOCUMENTATION);
    assertEquals(changeEvent.getEntityUrn(), TEST_ENTITY_URN);
    assertEquals(changeEvent.getModifier(), expectedModifier);
    assertNotNull(changeEvent.getParameters());
    assertEquals(changeEvent.getParameters().get("documentation"), expectedDoc);
  }

  @Test
  public void testAddDocumentationWithSource() throws Exception {
    DocumentationChangeEventGenerator generator = new DocumentationChangeEventGenerator();

    Urn urn = getTestUrn();
    AuditStamp auditStamp = getTestAuditStamp();

    Aspect<Documentation> from = createDocumentationAspect(List.of());
    Aspect<Documentation> to =
        createDocumentationAspect(
            List.of(
                createAssociationWithSource(
                    "Some documentation text", TEST_ACTOR_URN_1, TEST_SOURCE_URN_1)));

    List<ChangeEvent> actual =
        generator.getChangeEvents(urn, "dataset", "documentation", from, to, auditStamp);

    assertEquals(actual.size(), 1);
    validateChangeEvent(
        actual.get(0), ChangeOperation.ADD, "Some documentation text", TEST_SOURCE_URN_1);
  }

  @Test
  public void testRemoveDocumentationWithSource() throws Exception {
    DocumentationChangeEventGenerator generator = new DocumentationChangeEventGenerator();

    Urn urn = getTestUrn();
    AuditStamp auditStamp = getTestAuditStamp();

    Aspect<Documentation> from =
        createDocumentationAspect(
            List.of(
                createAssociationWithSource(
                    "Some documentation text", TEST_ACTOR_URN_1, TEST_SOURCE_URN_1)));
    Aspect<Documentation> to = createDocumentationAspect(List.of());

    List<ChangeEvent> actual =
        generator.getChangeEvents(urn, "dataset", "documentation", from, to, auditStamp);

    assertEquals(actual.size(), 1);
    validateChangeEvent(
        actual.get(0), ChangeOperation.REMOVE, "Some documentation text", TEST_SOURCE_URN_1);
  }

  @Test
  public void testModifyDocumentationWithSource() throws Exception {
    DocumentationChangeEventGenerator generator = new DocumentationChangeEventGenerator();

    Urn urn = getTestUrn();
    AuditStamp auditStamp = getTestAuditStamp();

    Aspect<Documentation> from =
        createDocumentationAspect(
            List.of(createAssociationWithSource("Old text", TEST_ACTOR_URN_1, TEST_SOURCE_URN_1)));
    Aspect<Documentation> to =
        createDocumentationAspect(
            List.of(createAssociationWithSource("New text", TEST_ACTOR_URN_1, TEST_SOURCE_URN_1)));

    List<ChangeEvent> actual =
        generator.getChangeEvents(urn, "dataset", "documentation", from, to, auditStamp);

    assertEquals(actual.size(), 1);
    validateChangeEvent(actual.get(0), ChangeOperation.MODIFY, "New text", TEST_SOURCE_URN_1);
  }

  @Test
  public void testNoChanges() throws Exception {
    DocumentationChangeEventGenerator generator = new DocumentationChangeEventGenerator();

    Urn urn = getTestUrn();
    AuditStamp auditStamp = getTestAuditStamp();

    Aspect<Documentation> from =
        createDocumentationAspect(
            List.of(createAssociationWithSource("Same text", TEST_ACTOR_URN_1, TEST_SOURCE_URN_1)));
    Aspect<Documentation> to =
        createDocumentationAspect(
            List.of(createAssociationWithSource("Same text", TEST_ACTOR_URN_1, TEST_SOURCE_URN_1)));

    List<ChangeEvent> actual =
        generator.getChangeEvents(urn, "dataset", "documentation", from, to, auditStamp);

    assertEquals(actual.size(), 0);
  }

  @Test
  public void testMultipleSourcesAddAndRemove() throws Exception {
    DocumentationChangeEventGenerator generator = new DocumentationChangeEventGenerator();

    Urn urn = getTestUrn();
    AuditStamp auditStamp = getTestAuditStamp();

    Aspect<Documentation> from =
        createDocumentationAspect(
            List.of(
                createAssociationWithSource(
                    "Doc from source1", TEST_ACTOR_URN_1, TEST_SOURCE_URN_1),
                createAssociationWithSource(
                    "Doc from source2", TEST_ACTOR_URN_2, TEST_SOURCE_URN_2)));
    Aspect<Documentation> to =
        createDocumentationAspect(
            List.of(
                createAssociationWithSource(
                    "Doc from source1", TEST_ACTOR_URN_1, TEST_SOURCE_URN_1)));

    List<ChangeEvent> actual =
        generator.getChangeEvents(urn, "dataset", "documentation", from, to, auditStamp);

    assertEquals(actual.size(), 1);
    validateChangeEvent(
        actual.get(0), ChangeOperation.REMOVE, "Doc from source2", TEST_SOURCE_URN_2);
  }

  @Test
  public void testAddSecondSource() throws Exception {
    DocumentationChangeEventGenerator generator = new DocumentationChangeEventGenerator();

    Urn urn = getTestUrn();
    AuditStamp auditStamp = getTestAuditStamp();

    Aspect<Documentation> from =
        createDocumentationAspect(
            List.of(
                createAssociationWithSource(
                    "Doc from source1", TEST_ACTOR_URN_1, TEST_SOURCE_URN_1)));
    Aspect<Documentation> to =
        createDocumentationAspect(
            List.of(
                createAssociationWithSource(
                    "Doc from source1", TEST_ACTOR_URN_1, TEST_SOURCE_URN_1),
                createAssociationWithSource(
                    "Doc from source2", TEST_ACTOR_URN_2, TEST_SOURCE_URN_2)));

    List<ChangeEvent> actual =
        generator.getChangeEvents(urn, "dataset", "documentation", from, to, auditStamp);

    assertEquals(actual.size(), 1);
    validateChangeEvent(actual.get(0), ChangeOperation.ADD, "Doc from source2", TEST_SOURCE_URN_2);
  }

  @Test
  public void testUnattributedDocUsesUnattributedKey() throws Exception {
    DocumentationChangeEventGenerator generator = new DocumentationChangeEventGenerator();

    Urn urn = getTestUrn();
    AuditStamp auditStamp = getTestAuditStamp();

    Aspect<Documentation> from = createDocumentationAspect(List.of());
    Aspect<Documentation> to =
        createDocumentationAspect(List.of(createAssociationNoAttribution("Plain doc text")));

    List<ChangeEvent> actual =
        generator.getChangeEvents(urn, "dataset", "documentation", from, to, auditStamp);

    assertEquals(actual.size(), 1);
    validateChangeEvent(actual.get(0), ChangeOperation.ADD, "Plain doc text", null);
  }

  @Test
  public void testUnattributedDocModified() throws Exception {
    DocumentationChangeEventGenerator generator = new DocumentationChangeEventGenerator();

    Urn urn = getTestUrn();
    AuditStamp auditStamp = getTestAuditStamp();

    Aspect<Documentation> from =
        createDocumentationAspect(List.of(createAssociationNoAttribution("Old unattributed")));
    Aspect<Documentation> to =
        createDocumentationAspect(List.of(createAssociationNoAttribution("New unattributed")));

    List<ChangeEvent> actual =
        generator.getChangeEvents(urn, "dataset", "documentation", from, to, auditStamp);

    assertEquals(actual.size(), 1);
    validateChangeEvent(actual.get(0), ChangeOperation.MODIFY, "New unattributed", null);
  }

  @Test
  public void testFromEmptyToMultiple() throws Exception {
    DocumentationChangeEventGenerator generator = new DocumentationChangeEventGenerator();

    Urn urn = getTestUrn();
    AuditStamp auditStamp = getTestAuditStamp();

    Aspect<Documentation> from = createDocumentationAspect(List.of());
    Aspect<Documentation> to =
        createDocumentationAspect(
            List.of(
                createAssociationWithSource("Doc 1", TEST_ACTOR_URN_1, TEST_SOURCE_URN_1),
                createAssociationWithSource("Doc 2", TEST_ACTOR_URN_2, TEST_SOURCE_URN_2)));

    List<ChangeEvent> actual =
        generator.getChangeEvents(urn, "dataset", "documentation", from, to, auditStamp);

    assertEquals(actual.size(), 2);
    Set<ChangeOperation> operations =
        actual.stream().map(ChangeEvent::getOperation).collect(Collectors.toSet());
    assertEquals(operations, Set.of(ChangeOperation.ADD));
    Set<String> docs =
        actual.stream()
            .map(e -> (String) e.getParameters().get("documentation"))
            .collect(Collectors.toSet());
    assertEquals(docs, Set.of("Doc 1", "Doc 2"));
  }

  @Test
  public void testFromMultipleToEmpty() throws Exception {
    DocumentationChangeEventGenerator generator = new DocumentationChangeEventGenerator();

    Urn urn = getTestUrn();
    AuditStamp auditStamp = getTestAuditStamp();

    Aspect<Documentation> from =
        createDocumentationAspect(
            List.of(
                createAssociationWithSource("Doc 1", TEST_ACTOR_URN_1, TEST_SOURCE_URN_1),
                createAssociationWithSource("Doc 2", TEST_ACTOR_URN_2, TEST_SOURCE_URN_2)));
    Aspect<Documentation> to = createDocumentationAspect(List.of());

    List<ChangeEvent> actual =
        generator.getChangeEvents(urn, "dataset", "documentation", from, to, auditStamp);

    assertEquals(actual.size(), 2);
    Set<ChangeOperation> operations =
        actual.stream().map(ChangeEvent::getOperation).collect(Collectors.toSet());
    assertEquals(operations, Set.of(ChangeOperation.REMOVE));
  }

  @Test
  public void testSwapSourcesWithDifferentDocs() throws Exception {
    DocumentationChangeEventGenerator generator = new DocumentationChangeEventGenerator();

    Urn urn = getTestUrn();
    AuditStamp auditStamp = getTestAuditStamp();

    // source1 removed, source2 added; both different text
    Aspect<Documentation> from =
        createDocumentationAspect(
            List.of(
                createAssociationWithSource(
                    "Doc from source1", TEST_ACTOR_URN_1, TEST_SOURCE_URN_1)));
    Aspect<Documentation> to =
        createDocumentationAspect(
            List.of(
                createAssociationWithSource(
                    "Doc from source2", TEST_ACTOR_URN_2, TEST_SOURCE_URN_2)));

    List<ChangeEvent> actual =
        generator.getChangeEvents(urn, "dataset", "documentation", from, to, auditStamp);

    assertEquals(actual.size(), 2);

    List<ChangeEvent> sorted =
        actual.stream()
            .sorted(Comparator.comparing(ChangeEvent::getOperation))
            .collect(Collectors.toList());

    validateChangeEvent(sorted.get(0), ChangeOperation.ADD, "Doc from source2", TEST_SOURCE_URN_2);
    validateChangeEvent(
        sorted.get(1), ChangeOperation.REMOVE, "Doc from source1", TEST_SOURCE_URN_1);
  }

  @Test
  public void testAddUnattributedDocAlongsideAttributed() throws Exception {
    DocumentationChangeEventGenerator generator = new DocumentationChangeEventGenerator();

    Urn urn = getTestUrn();
    AuditStamp auditStamp = getTestAuditStamp();

    Aspect<Documentation> from =
        createDocumentationAspect(
            List.of(
                createAssociationWithSource(
                    "Attributed doc", TEST_ACTOR_URN_1, TEST_SOURCE_URN_1)));
    Aspect<Documentation> to =
        createDocumentationAspect(
            List.of(
                createAssociationWithSource("Attributed doc", TEST_ACTOR_URN_1, TEST_SOURCE_URN_1),
                createAssociationNoAttribution("Unattributed doc")));

    List<ChangeEvent> actual =
        generator.getChangeEvents(urn, "dataset", "documentation", from, to, auditStamp);

    assertEquals(actual.size(), 1);
    validateChangeEvent(actual.get(0), ChangeOperation.ADD, "Unattributed doc", null);
  }

  @Test
  public void testRemoveUnattributedDoc() throws Exception {
    DocumentationChangeEventGenerator generator = new DocumentationChangeEventGenerator();

    Urn urn = getTestUrn();
    AuditStamp auditStamp = getTestAuditStamp();

    Aspect<Documentation> from =
        createDocumentationAspect(List.of(createAssociationNoAttribution("Unattributed doc")));
    Aspect<Documentation> to = createDocumentationAspect(List.of());

    List<ChangeEvent> actual =
        generator.getChangeEvents(urn, "dataset", "documentation", from, to, auditStamp);

    assertEquals(actual.size(), 1);
    validateChangeEvent(actual.get(0), ChangeOperation.REMOVE, "Unattributed doc", null);
  }

  @Test
  public void testDescriptionFormat() throws Exception {
    DocumentationChangeEventGenerator generator = new DocumentationChangeEventGenerator();

    Urn urn = getTestUrn();
    AuditStamp auditStamp = getTestAuditStamp();

    Aspect<Documentation> from = createDocumentationAspect(List.of());
    Aspect<Documentation> to =
        createDocumentationAspect(
            List.of(
                createAssociationWithSource("My doc text", TEST_ACTOR_URN_1, TEST_SOURCE_URN_1)));

    List<ChangeEvent> actual =
        generator.getChangeEvents(urn, "dataset", "documentation", from, to, auditStamp);

    assertEquals(actual.size(), 1);
    assertNotNull(actual.get(0).getDescription());
    assertEquals(
        actual.get(0).getDescription(),
        String.format("Documentation for '%s' has been added: 'My doc text'.", TEST_ENTITY_URN));
  }
}
