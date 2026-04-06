package com.linkedin.metadata.timeline.eventgenerator;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.GlossaryTermAssociation;
import com.linkedin.common.GlossaryTermAssociationArray;
import com.linkedin.common.GlossaryTerms;
import com.linkedin.common.urn.GlossaryTermUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.timeline.data.ChangeEvent;
import com.linkedin.metadata.timeline.data.ChangeOperation;
import com.linkedin.metadata.timeline.data.entity.GlossaryTermChangeEvent;
import com.linkedin.mxe.SystemMetadata;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

public class GlossaryTermsChangeEventGeneratorTest extends AbstractTestNGSpringContextTests {

  private static final String TEST_ENTITY_URN =
      "urn:li:dataset:(urn:li:dataPlatform:hive,SampleTable,PROD)";
  private static final String TEST_GLOSSARY_TERM_URN_1 = "urn:li:glossaryTerm:Test.Term1";
  private static final String TEST_GLOSSARY_TERM_URN_2 = "urn:li:glossaryTerm:Test.Term2";
  private static final String TEST_GLOSSARY_TERM_URN_3 = "urn:li:glossaryTerm:Test.Term3";
  private static final String TEST_CONTEXT_1 =
      "{\"origin\":\"urn:li:dataset:(urn:li:dataPlatform:hive,OriginTable,PROD)\",\"propagated\":\"true\",\"actor\":\"urn:li:corpuser:test\"}";
  private static final String TEST_CONTEXT_2 =
      "{\"origin\":\"urn:li:dataset:(urn:li:dataPlatform:hive,OriginTable2,PROD)\",\"propagated\":\"true\",\"actor\":\"urn:li:corpuser:test2\"}";

  private static Urn getTestUrn() throws URISyntaxException {
    return Urn.createFromString(TEST_ENTITY_URN);
  }

  private static AuditStamp getTestAuditStamp() throws URISyntaxException {
    return new AuditStamp()
        .setActor(Urn.createFromString("urn:li:corpuser:__datahub_system"))
        .setTime(1683829509553L);
  }

  private static Aspect<GlossaryTerms> createGlossaryTermsAspect(
      List<GlossaryTermAssociation> termAssociations) {
    GlossaryTerms glossaryTerms = new GlossaryTerms();
    glossaryTerms.setTerms(new GlossaryTermAssociationArray(termAssociations));
    return new Aspect<>(glossaryTerms, new SystemMetadata());
  }

  private static GlossaryTermAssociation createTermAssociation(String termUrn, String context)
      throws URISyntaxException {
    GlossaryTermAssociation association = new GlossaryTermAssociation();
    association.setUrn(GlossaryTermUrn.createFromString(termUrn));
    if (context != null) {
      association.setContext(context);
    }
    return association;
  }

  private static void validateChangeEvent(
      ChangeEvent changeEvent,
      ChangeOperation expectedOperation,
      String expectedTermUrn,
      String expectedContext) {
    assertNotNull(changeEvent);
    assertEquals(changeEvent.getOperation(), expectedOperation);
    assertEquals(changeEvent.getModifier(), expectedTermUrn);
    assertEquals(changeEvent.getEntityUrn(), TEST_ENTITY_URN);

    assertNotNull(changeEvent.getParameters());
    assertEquals(changeEvent.getParameters().get("termUrn"), expectedTermUrn);
    assertEquals(
        changeEvent.getParameters().get("context"),
        expectedContext != null ? expectedContext : "{}");
  }

  @Test
  public void testAddGlossaryTermWithContext() throws Exception {
    GlossaryTermsChangeEventGenerator generator = new GlossaryTermsChangeEventGenerator();

    Urn urn = getTestUrn();
    String entity = "dataset";
    String aspect = "glossaryTerms";
    AuditStamp auditStamp = getTestAuditStamp();

    // From: empty
    Aspect<GlossaryTerms> from = createGlossaryTermsAspect(List.of());

    // To: one term with context
    Aspect<GlossaryTerms> to =
        createGlossaryTermsAspect(
            List.of(createTermAssociation(TEST_GLOSSARY_TERM_URN_1, TEST_CONTEXT_1)));

    List<ChangeEvent> actual = generator.getChangeEvents(urn, entity, aspect, from, to, auditStamp);

    assertEquals(actual.size(), 1);
    validateChangeEvent(
        actual.get(0), ChangeOperation.ADD, TEST_GLOSSARY_TERM_URN_1, TEST_CONTEXT_1);
  }

  @Test
  public void testAddGlossaryTermWithoutContext() throws Exception {
    GlossaryTermsChangeEventGenerator generator = new GlossaryTermsChangeEventGenerator();

    Urn urn = getTestUrn();
    String entity = "dataset";
    String aspect = "glossaryTerms";
    AuditStamp auditStamp = getTestAuditStamp();

    // From: empty
    Aspect<GlossaryTerms> from = createGlossaryTermsAspect(List.of());

    // To: one term without context
    Aspect<GlossaryTerms> to =
        createGlossaryTermsAspect(List.of(createTermAssociation(TEST_GLOSSARY_TERM_URN_1, null)));

    List<ChangeEvent> actual = generator.getChangeEvents(urn, entity, aspect, from, to, auditStamp);

    assertEquals(actual.size(), 1);
    validateChangeEvent(actual.get(0), ChangeOperation.ADD, TEST_GLOSSARY_TERM_URN_1, null);
  }

  @Test
  public void testRemoveGlossaryTermWithContext() throws Exception {
    GlossaryTermsChangeEventGenerator generator = new GlossaryTermsChangeEventGenerator();

    Urn urn = getTestUrn();
    String entity = "dataset";
    String aspect = "glossaryTerms";
    AuditStamp auditStamp = getTestAuditStamp();

    // From: one term with context
    Aspect<GlossaryTerms> from =
        createGlossaryTermsAspect(
            List.of(createTermAssociation(TEST_GLOSSARY_TERM_URN_1, TEST_CONTEXT_1)));

    // To: empty
    Aspect<GlossaryTerms> to = createGlossaryTermsAspect(List.of());

    List<ChangeEvent> actual = generator.getChangeEvents(urn, entity, aspect, from, to, auditStamp);

    assertEquals(actual.size(), 1);
    validateChangeEvent(
        actual.get(0), ChangeOperation.REMOVE, TEST_GLOSSARY_TERM_URN_1, TEST_CONTEXT_1);
  }

  @Test
  public void testMultipleTermsWithContextPropagation() throws Exception {
    GlossaryTermsChangeEventGenerator generator = new GlossaryTermsChangeEventGenerator();

    Urn urn = getTestUrn();
    String entity = "dataset";
    String aspect = "glossaryTerms";
    AuditStamp auditStamp = getTestAuditStamp();

    // From: one term with context
    Aspect<GlossaryTerms> from =
        createGlossaryTermsAspect(
            List.of(createTermAssociation(TEST_GLOSSARY_TERM_URN_1, TEST_CONTEXT_1)));

    // To: different term with different context and additional term without context
    Aspect<GlossaryTerms> to =
        createGlossaryTermsAspect(
            List.of(
                createTermAssociation(TEST_GLOSSARY_TERM_URN_2, TEST_CONTEXT_2),
                createTermAssociation(TEST_GLOSSARY_TERM_URN_3, null)));

    List<ChangeEvent> actual = generator.getChangeEvents(urn, entity, aspect, from, to, auditStamp);

    assertEquals(actual.size(), 3);

    // Sort by operation and term URN for consistent testing
    List<ChangeEvent> sortedEvents =
        actual.stream()
            .sorted(
                (e1, e2) -> {
                  int opCompare = e1.getOperation().compareTo(e2.getOperation());
                  if (opCompare != 0) return opCompare;
                  return e1.getModifier().compareTo(e2.getModifier());
                })
            .collect(Collectors.toList());

    // First event should be ADD of Term2
    validateChangeEvent(
        sortedEvents.get(0), ChangeOperation.ADD, TEST_GLOSSARY_TERM_URN_2, TEST_CONTEXT_2);

    // Second event should be ADD of Term3
    validateChangeEvent(sortedEvents.get(1), ChangeOperation.ADD, TEST_GLOSSARY_TERM_URN_3, null);

    // Third event should be REMOVE of Term1
    validateChangeEvent(
        sortedEvents.get(2), ChangeOperation.REMOVE, TEST_GLOSSARY_TERM_URN_1, TEST_CONTEXT_1);
  }

  @Test
  public void testNoChanges() throws Exception {
    GlossaryTermsChangeEventGenerator generator = new GlossaryTermsChangeEventGenerator();

    Urn urn = getTestUrn();
    String entity = "dataset";
    String aspect = "glossaryTerms";
    AuditStamp auditStamp = getTestAuditStamp();

    // From and To: same term with same context
    Aspect<GlossaryTerms> from =
        createGlossaryTermsAspect(
            List.of(createTermAssociation(TEST_GLOSSARY_TERM_URN_1, TEST_CONTEXT_1)));

    Aspect<GlossaryTerms> to =
        createGlossaryTermsAspect(
            List.of(createTermAssociation(TEST_GLOSSARY_TERM_URN_1, TEST_CONTEXT_1)));

    List<ChangeEvent> actual = generator.getChangeEvents(urn, entity, aspect, from, to, auditStamp);

    assertEquals(actual.size(), 0);
  }

  @Test
  public void testContextPropagationFromEmptyToMultipleTerms() throws Exception {
    GlossaryTermsChangeEventGenerator generator = new GlossaryTermsChangeEventGenerator();

    Urn urn = getTestUrn();
    String entity = "dataset";
    String aspect = "glossaryTerms";
    AuditStamp auditStamp = getTestAuditStamp();

    // From: empty
    Aspect<GlossaryTerms> from = createGlossaryTermsAspect(List.of());

    // To: multiple terms with different contexts
    Aspect<GlossaryTerms> to =
        createGlossaryTermsAspect(
            List.of(
                createTermAssociation(TEST_GLOSSARY_TERM_URN_1, TEST_CONTEXT_1),
                createTermAssociation(TEST_GLOSSARY_TERM_URN_2, TEST_CONTEXT_2),
                createTermAssociation(TEST_GLOSSARY_TERM_URN_3, null)));

    List<ChangeEvent> actual = generator.getChangeEvents(urn, entity, aspect, from, to, auditStamp);

    assertEquals(actual.size(), 3);

    // All should be ADD operations
    Set<ChangeOperation> operations =
        actual.stream().map(ChangeEvent::getOperation).collect(Collectors.toSet());
    assertEquals(operations, Set.of(ChangeOperation.ADD));

    // Verify all terms are present with correct contexts
    Set<String> actualTermUrns =
        actual.stream().map(ChangeEvent::getModifier).collect(Collectors.toSet());
    assertEquals(
        actualTermUrns,
        Set.of(TEST_GLOSSARY_TERM_URN_1, TEST_GLOSSARY_TERM_URN_2, TEST_GLOSSARY_TERM_URN_3));
  }

  @Test
  public void testContextPropagationFromMultipleTermsToEmpty() throws Exception {
    GlossaryTermsChangeEventGenerator generator = new GlossaryTermsChangeEventGenerator();

    Urn urn = getTestUrn();
    String entity = "dataset";
    String aspect = "glossaryTerms";
    AuditStamp auditStamp = getTestAuditStamp();

    // From: multiple terms with different contexts
    Aspect<GlossaryTerms> from =
        createGlossaryTermsAspect(
            List.of(
                createTermAssociation(TEST_GLOSSARY_TERM_URN_1, TEST_CONTEXT_1),
                createTermAssociation(TEST_GLOSSARY_TERM_URN_2, TEST_CONTEXT_2),
                createTermAssociation(TEST_GLOSSARY_TERM_URN_3, null)));

    // To: empty
    Aspect<GlossaryTerms> to = createGlossaryTermsAspect(List.of());

    List<ChangeEvent> actual = generator.getChangeEvents(urn, entity, aspect, from, to, auditStamp);

    assertEquals(actual.size(), 3);

    // All should be REMOVE operations
    Set<ChangeOperation> operations =
        actual.stream().map(ChangeEvent::getOperation).collect(Collectors.toSet());
    assertEquals(operations, Set.of(ChangeOperation.REMOVE));

    // Verify all terms are present with correct contexts
    Set<String> actualTermUrns =
        actual.stream().map(ChangeEvent::getModifier).collect(Collectors.toSet());
    assertEquals(
        actualTermUrns,
        Set.of(TEST_GLOSSARY_TERM_URN_1, TEST_GLOSSARY_TERM_URN_2, TEST_GLOSSARY_TERM_URN_3));
  }

  @Test
  public void testChangeEventIsGlossaryTermChangeEvent() throws Exception {
    GlossaryTermsChangeEventGenerator generator = new GlossaryTermsChangeEventGenerator();

    Urn urn = getTestUrn();
    String entity = "dataset";
    String aspect = "glossaryTerms";
    AuditStamp auditStamp = getTestAuditStamp();

    // From: empty
    Aspect<GlossaryTerms> from = createGlossaryTermsAspect(List.of());

    // To: one term
    Aspect<GlossaryTerms> to =
        createGlossaryTermsAspect(
            List.of(createTermAssociation(TEST_GLOSSARY_TERM_URN_1, TEST_CONTEXT_1)));

    List<ChangeEvent> actual = generator.getChangeEvents(urn, entity, aspect, from, to, auditStamp);

    assertEquals(actual.size(), 1);

    // Verify the change event is of correct type
    ChangeEvent changeEvent = actual.get(0);
    assertEquals(changeEvent.getClass(), GlossaryTermChangeEvent.class);

    GlossaryTermChangeEvent glossaryTermChangeEvent = (GlossaryTermChangeEvent) changeEvent;
    assertNotNull(glossaryTermChangeEvent.getParameters());
    assertEquals(glossaryTermChangeEvent.getParameters().get("termUrn"), TEST_GLOSSARY_TERM_URN_1);
    assertEquals(glossaryTermChangeEvent.getParameters().get("context"), TEST_CONTEXT_1);
  }
}
