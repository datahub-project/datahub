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
import com.linkedin.metadata.timeline.data.dataset.schema.SchemaFieldGlossaryTermChangeEvent;
import com.linkedin.metadata.timeline.data.entity.GlossaryTermChangeEvent;
import com.linkedin.mxe.SystemMetadata;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

public class GlossaryTermContextPropagationTest extends AbstractTestNGSpringContextTests {

  private static final String TEST_ENTITY_URN =
      "urn:li:dataset:(urn:li:dataPlatform:hive,SampleTable,PROD)";
  private static final String TEST_PARENT_URN =
      "urn:li:dataset:(urn:li:dataPlatform:hive,ParentTable,PROD)";
  private static final String TEST_FIELD_PATH = "field1";
  private static final String TEST_GLOSSARY_TERM_URN = "urn:li:glossaryTerm:Test.Term";
  private static final String TEST_CONTEXT =
      "{\"origin\":\"urn:li:dataset:(urn:li:dataPlatform:hive,OriginTable,PROD)\",\"propagated\":\"true\",\"actor\":\"urn:li:corpuser:test\"}";

  private static AuditStamp getTestAuditStamp() throws URISyntaxException {
    return new AuditStamp()
        .setActor(Urn.createFromString("urn:li:corpuser:__datahub_system"))
        .setTime(1683829509553L);
  }

  @Test
  public void testEntityGlossaryTermChangeEventWithContext() throws Exception {
    AuditStamp auditStamp = getTestAuditStamp();

    // Test that GlossaryTermChangeEvent can be created with context in parameters
    Map<String, Object> parameters =
        com.google.common.collect.ImmutableMap.of(
            "termUrn", TEST_GLOSSARY_TERM_URN,
            "context", TEST_CONTEXT);

    GlossaryTermChangeEvent event =
        GlossaryTermChangeEvent.entityGlossaryTermChangeEventBuilder()
            .entityUrn(TEST_ENTITY_URN)
            .category(com.linkedin.metadata.timeline.data.ChangeCategory.GLOSSARY_TERM)
            .operation(ChangeOperation.ADD)
            .modifier(TEST_GLOSSARY_TERM_URN)
            .parameters(parameters)
            .auditStamp(auditStamp)
            .semVerChange(com.linkedin.metadata.timeline.data.SemanticChangeType.MINOR)
            .description("Test glossary term added")
            .build();

    assertNotNull(event);
    assertEquals(event.getEntityUrn(), TEST_ENTITY_URN);
    assertEquals(event.getModifier(), TEST_GLOSSARY_TERM_URN);
    assertEquals(event.getOperation(), ChangeOperation.ADD);

    Map<String, Object> eventParameters = event.getParameters();
    assertNotNull(eventParameters);
    assertEquals(eventParameters.get("termUrn"), TEST_GLOSSARY_TERM_URN);
    assertEquals(eventParameters.get("context"), TEST_CONTEXT);
  }

  @Test
  public void testSchemaFieldGlossaryTermChangeEventWithContext() throws Exception {
    AuditStamp auditStamp = getTestAuditStamp();

    // Test that SchemaFieldGlossaryTermChangeEvent can be created with context in parameters
    Map<String, Object> parameters =
        com.google.common.collect.ImmutableMap.of(
            "fieldPath", TEST_FIELD_PATH,
            "parentUrn", TEST_PARENT_URN,
            "termUrn", TEST_GLOSSARY_TERM_URN,
            "context", TEST_CONTEXT);

    SchemaFieldGlossaryTermChangeEvent event =
        SchemaFieldGlossaryTermChangeEvent.schemaFieldGlossaryTermChangeEventBuilder()
            .entityUrn(TEST_ENTITY_URN)
            .category(com.linkedin.metadata.timeline.data.ChangeCategory.GLOSSARY_TERM)
            .operation(ChangeOperation.ADD)
            .modifier(TEST_GLOSSARY_TERM_URN)
            .auditStamp(auditStamp)
            .semVerChange(com.linkedin.metadata.timeline.data.SemanticChangeType.MINOR)
            .description("Test glossary term added")
            .parameters(parameters)
            .build();

    assertNotNull(event);
    assertEquals(event.getEntityUrn(), TEST_ENTITY_URN);
    assertEquals(event.getModifier(), TEST_GLOSSARY_TERM_URN);
    assertEquals(event.getOperation(), ChangeOperation.ADD);

    Map<String, Object> eventParameters = event.getParameters();
    assertNotNull(eventParameters);
    assertEquals(eventParameters.get("fieldPath"), TEST_FIELD_PATH);
    assertEquals(eventParameters.get("parentUrn"), TEST_PARENT_URN);
    assertEquals(eventParameters.get("termUrn"), TEST_GLOSSARY_TERM_URN);
    assertEquals(eventParameters.get("context"), TEST_CONTEXT);
  }

  @Test
  public void testSchemaFieldGlossaryTermChangeEventWithoutContext() throws Exception {
    AuditStamp auditStamp = getTestAuditStamp();

    // Test that SchemaFieldGlossaryTermChangeEvent handles null context properly
    Map<String, Object> parameters =
        com.google.common.collect.ImmutableMap.of(
            "fieldPath", TEST_FIELD_PATH,
            "parentUrn", TEST_PARENT_URN,
            "termUrn", TEST_GLOSSARY_TERM_URN,
            "context", "{}");

    SchemaFieldGlossaryTermChangeEvent event =
        SchemaFieldGlossaryTermChangeEvent.schemaFieldGlossaryTermChangeEventBuilder()
            .entityUrn(TEST_ENTITY_URN)
            .category(com.linkedin.metadata.timeline.data.ChangeCategory.GLOSSARY_TERM)
            .operation(ChangeOperation.ADD)
            .modifier(TEST_GLOSSARY_TERM_URN)
            .auditStamp(auditStamp)
            .semVerChange(com.linkedin.metadata.timeline.data.SemanticChangeType.MINOR)
            .description("Test glossary term added")
            .parameters(parameters)
            .build();

    assertNotNull(event);

    Map<String, Object> eventParameters = event.getParameters();
    assertNotNull(eventParameters);
    assertEquals(eventParameters.get("fieldPath"), TEST_FIELD_PATH);
    assertEquals(eventParameters.get("parentUrn"), TEST_PARENT_URN);
    assertEquals(eventParameters.get("termUrn"), TEST_GLOSSARY_TERM_URN);
    assertEquals(eventParameters.get("context"), "{}");
  }

  @Test
  public void testGlossaryTermsChangeEventGeneratorWithContext() throws Exception {
    GlossaryTermsChangeEventGenerator generator = new GlossaryTermsChangeEventGenerator();

    Urn urn = Urn.createFromString(TEST_ENTITY_URN);
    String entity = "dataset";
    String aspect = "glossaryTerms";
    AuditStamp auditStamp = getTestAuditStamp();

    // Create term association with context
    GlossaryTermAssociation termAssociation = new GlossaryTermAssociation();
    termAssociation.setUrn(GlossaryTermUrn.createFromString(TEST_GLOSSARY_TERM_URN));
    termAssociation.setContext(TEST_CONTEXT);

    // From: empty
    Aspect<GlossaryTerms> from =
        new Aspect<>(
            new GlossaryTerms().setTerms(new GlossaryTermAssociationArray()), new SystemMetadata());

    // To: one term with context
    Aspect<GlossaryTerms> to =
        new Aspect<>(
            new GlossaryTerms()
                .setTerms(new GlossaryTermAssociationArray(List.of(termAssociation))),
            new SystemMetadata());

    List<ChangeEvent> actual = generator.getChangeEvents(urn, entity, aspect, from, to, auditStamp);

    assertEquals(actual.size(), 1);

    ChangeEvent event = actual.get(0);
    assertEquals(event.getClass(), GlossaryTermChangeEvent.class);
    assertEquals(event.getOperation(), ChangeOperation.ADD);
    assertEquals(event.getModifier(), TEST_GLOSSARY_TERM_URN);

    Map<String, Object> eventParameters = event.getParameters();
    assertNotNull(eventParameters);
    assertEquals(eventParameters.get("termUrn"), TEST_GLOSSARY_TERM_URN);
    assertEquals(eventParameters.get("context"), TEST_CONTEXT);
  }

  @Test
  public void testConvertEntityToSchemaFieldEventWithContext() throws Exception {
    AuditStamp auditStamp = getTestAuditStamp();

    // Create entity-level event with context
    Map<String, Object> entityParameters =
        com.google.common.collect.ImmutableMap.of(
            "termUrn", TEST_GLOSSARY_TERM_URN,
            "context", TEST_CONTEXT);

    GlossaryTermChangeEvent entityEvent =
        GlossaryTermChangeEvent.entityGlossaryTermChangeEventBuilder()
            .entityUrn(TEST_ENTITY_URN)
            .category(com.linkedin.metadata.timeline.data.ChangeCategory.GLOSSARY_TERM)
            .operation(ChangeOperation.ADD)
            .modifier(TEST_GLOSSARY_TERM_URN)
            .parameters(entityParameters)
            .auditStamp(auditStamp)
            .semVerChange(com.linkedin.metadata.timeline.data.SemanticChangeType.MINOR)
            .description("Test glossary term added")
            .build();

    // Convert to schema field event
    List<ChangeEvent> result =
        ChangeEventGeneratorUtils.convertEntityGlossaryTermChangeEvents(
            TEST_FIELD_PATH, Urn.createFromString(TEST_PARENT_URN), List.of(entityEvent));

    assertEquals(result.size(), 1);

    ChangeEvent schemaFieldEvent = result.get(0);
    assertEquals(schemaFieldEvent.getClass(), SchemaFieldGlossaryTermChangeEvent.class);

    SchemaFieldGlossaryTermChangeEvent glossaryTermEvent =
        (SchemaFieldGlossaryTermChangeEvent) schemaFieldEvent;

    // Verify context is preserved in the converted event
    Map<String, Object> parameters = glossaryTermEvent.getParameters();
    assertNotNull(parameters);
    assertEquals(parameters.get("fieldPath"), TEST_FIELD_PATH);
    assertEquals(parameters.get("parentUrn"), TEST_PARENT_URN);
    assertEquals(parameters.get("termUrn"), TEST_GLOSSARY_TERM_URN);
    assertEquals(parameters.get("context"), TEST_CONTEXT);
  }
}
