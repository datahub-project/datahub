package com.linkedin.metadata.timeline.eventgenerator;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.GlossaryTermAssociation;
import com.linkedin.common.TagAssociation;
import com.linkedin.common.urn.GlossaryTermUrn;
import com.linkedin.common.urn.TagUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.timeline.data.ChangeCategory;
import com.linkedin.metadata.timeline.data.ChangeEvent;
import com.linkedin.metadata.timeline.data.ChangeOperation;
import com.linkedin.metadata.timeline.data.SemanticChangeType;
import com.linkedin.metadata.timeline.data.dataset.schema.SchemaFieldGlossaryTermChangeEvent;
import com.linkedin.metadata.timeline.data.dataset.schema.SchemaFieldTagChangeEvent;
import com.linkedin.metadata.timeline.data.entity.GlossaryTermChangeEvent;
import com.linkedin.metadata.timeline.data.entity.TagChangeEvent;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

public class ChangeEventGeneratorUtilsTest extends AbstractTestNGSpringContextTests {

  private static final String TEST_ENTITY_URN =
      "urn:li:dataset:(urn:li:dataPlatform:hive,SampleTable,PROD)";
  private static final String TEST_PARENT_URN =
      "urn:li:dataset:(urn:li:dataPlatform:hive,ParentTable,PROD)";
  private static final String TEST_FIELD_PATH = "field1";
  private static final String TEST_TAG_URN = "urn:li:tag:TestTag";
  private static final String TEST_GLOSSARY_TERM_URN = "urn:li:glossaryTerm:Test.Term";
  private static final String TEST_CONTEXT =
      "{\"origin\":\"urn:li:dataset:(urn:li:dataPlatform:hive,OriginTable,PROD)\",\"propagated\":\"true\",\"actor\":\"urn:li:corpuser:test\"}";

  private static AuditStamp getTestAuditStamp() throws URISyntaxException {
    return new AuditStamp()
        .setActor(Urn.createFromString("urn:li:corpuser:__datahub_system"))
        .setTime(1683829509553L);
  }

  @Test
  public void testConvertEntityTagChangeEventsWithContext() throws Exception {
    AuditStamp auditStamp = getTestAuditStamp();

    // Create a TagChangeEvent with context
    TagChangeEvent tagChangeEvent =
        TagChangeEvent.entityTagChangeEventBuilder()
            .entityUrn(TEST_ENTITY_URN)
            .category(ChangeCategory.TAG)
            .operation(ChangeOperation.ADD)
            .modifier(TEST_TAG_URN)
            .tagAssociation(
                new TagAssociation()
                    .setTag(TagUrn.createFromString(TEST_TAG_URN))
                    .setContext(TEST_CONTEXT))
            .auditStamp(auditStamp)
            .semVerChange(SemanticChangeType.MINOR)
            .description("Test tag added")
            .build();

    List<ChangeEvent> result =
        ChangeEventGeneratorUtils.convertEntityTagChangeEvents(
            TEST_FIELD_PATH, Urn.createFromString(TEST_PARENT_URN), List.of(tagChangeEvent));

    assertEquals(result.size(), 1);

    ChangeEvent schemaFieldEvent = result.get(0);
    assertEquals(schemaFieldEvent.getClass(), SchemaFieldTagChangeEvent.class);

    SchemaFieldTagChangeEvent schemaFieldTagEvent = (SchemaFieldTagChangeEvent) schemaFieldEvent;
    assertNotNull(schemaFieldTagEvent.getParameters());

    Map<String, Object> parameters = schemaFieldTagEvent.getParameters();
    assertEquals(parameters.get("fieldPath"), TEST_FIELD_PATH);
    assertEquals(parameters.get("parentUrn"), TEST_PARENT_URN);
    assertEquals(parameters.get("tagUrn"), TEST_TAG_URN);
    assertEquals(parameters.get("context"), TEST_CONTEXT);

    assertEquals(schemaFieldTagEvent.getEntityUrn(), TEST_ENTITY_URN);
    assertEquals(schemaFieldTagEvent.getCategory(), ChangeCategory.TAG);
    assertEquals(schemaFieldTagEvent.getOperation(), ChangeOperation.ADD);
    assertEquals(schemaFieldTagEvent.getModifier(), TEST_TAG_URN);
  }

  @Test
  public void testConvertEntityGlossaryTermChangeEventsWithContext() throws Exception {
    AuditStamp auditStamp = getTestAuditStamp();

    // Create a GlossaryTermChangeEvent with context
    GlossaryTermChangeEvent glossaryTermChangeEvent =
        GlossaryTermChangeEvent.entityGlossaryTermChangeEventBuilder()
            .entityUrn(TEST_ENTITY_URN)
            .category(ChangeCategory.GLOSSARY_TERM)
            .operation(ChangeOperation.ADD)
            .modifier(TEST_GLOSSARY_TERM_URN)
            .glossaryTermAssociation(
                new GlossaryTermAssociation()
                    .setUrn(GlossaryTermUrn.createFromString(TEST_GLOSSARY_TERM_URN))
                    .setContext(TEST_CONTEXT))
            .auditStamp(auditStamp)
            .semVerChange(SemanticChangeType.MINOR)
            .description("Test glossary term added")
            .build();

    List<ChangeEvent> result =
        ChangeEventGeneratorUtils.convertEntityGlossaryTermChangeEvents(
            TEST_FIELD_PATH,
            Urn.createFromString(TEST_PARENT_URN),
            List.of(glossaryTermChangeEvent));

    assertEquals(result.size(), 1);

    ChangeEvent schemaFieldEvent = result.get(0);
    assertEquals(schemaFieldEvent.getClass(), SchemaFieldGlossaryTermChangeEvent.class);

    SchemaFieldGlossaryTermChangeEvent schemaFieldGlossaryTermEvent =
        (SchemaFieldGlossaryTermChangeEvent) schemaFieldEvent;
    assertNotNull(schemaFieldGlossaryTermEvent.getParameters());

    Map<String, Object> eventParameters = schemaFieldGlossaryTermEvent.getParameters();
    assertEquals(eventParameters.get("fieldPath"), TEST_FIELD_PATH);
    assertEquals(eventParameters.get("parentUrn"), TEST_PARENT_URN);
    assertEquals(eventParameters.get("termUrn"), TEST_GLOSSARY_TERM_URN);
    assertEquals(eventParameters.get("context"), TEST_CONTEXT);

    assertEquals(schemaFieldGlossaryTermEvent.getEntityUrn(), TEST_ENTITY_URN);
    assertEquals(schemaFieldGlossaryTermEvent.getCategory(), ChangeCategory.GLOSSARY_TERM);
    assertEquals(schemaFieldGlossaryTermEvent.getOperation(), ChangeOperation.ADD);
    assertEquals(schemaFieldGlossaryTermEvent.getModifier(), TEST_GLOSSARY_TERM_URN);
  }

  @Test
  public void testConvertEntityTagChangeEventsWithoutContext() throws Exception {
    AuditStamp auditStamp = getTestAuditStamp();

    // Create a TagChangeEvent without context
    TagChangeEvent tagChangeEvent =
        TagChangeEvent.entityTagChangeEventBuilder()
            .entityUrn(TEST_ENTITY_URN)
            .category(ChangeCategory.TAG)
            .operation(ChangeOperation.REMOVE)
            .modifier(TEST_TAG_URN)
            .tagAssociation(new TagAssociation().setTag(TagUrn.createFromString(TEST_TAG_URN)))
            .auditStamp(auditStamp)
            .semVerChange(SemanticChangeType.MINOR)
            .description("Test tag removed")
            .build();

    List<ChangeEvent> result =
        ChangeEventGeneratorUtils.convertEntityTagChangeEvents(
            TEST_FIELD_PATH, Urn.createFromString(TEST_PARENT_URN), List.of(tagChangeEvent));

    assertEquals(result.size(), 1);

    ChangeEvent schemaFieldEvent = result.get(0);
    assertEquals(schemaFieldEvent.getClass(), SchemaFieldTagChangeEvent.class);

    SchemaFieldTagChangeEvent schemaFieldTagEvent = (SchemaFieldTagChangeEvent) schemaFieldEvent;
    assertNotNull(schemaFieldTagEvent.getParameters());

    Map<String, Object> parameters = schemaFieldTagEvent.getParameters();
    assertEquals(parameters.get("fieldPath"), TEST_FIELD_PATH);
    assertEquals(parameters.get("parentUrn"), TEST_PARENT_URN);
    assertEquals(parameters.get("tagUrn"), TEST_TAG_URN);
    assertEquals(parameters.get("context"), "{}");

    assertEquals(schemaFieldTagEvent.getOperation(), ChangeOperation.REMOVE);
  }

  @Test
  public void testConvertEntityGlossaryTermChangeEventsWithoutContext() throws Exception {
    AuditStamp auditStamp = getTestAuditStamp();

    // Create a GlossaryTermChangeEvent without context
    GlossaryTermChangeEvent glossaryTermChangeEvent =
        GlossaryTermChangeEvent.entityGlossaryTermChangeEventBuilder()
            .entityUrn(TEST_ENTITY_URN)
            .category(ChangeCategory.GLOSSARY_TERM)
            .operation(ChangeOperation.REMOVE)
            .modifier(TEST_GLOSSARY_TERM_URN)
            .glossaryTermAssociation(
                new GlossaryTermAssociation()
                    .setUrn(GlossaryTermUrn.createFromString(TEST_GLOSSARY_TERM_URN)))
            .auditStamp(auditStamp)
            .semVerChange(SemanticChangeType.MINOR)
            .description("Test glossary term removed")
            .build();

    List<ChangeEvent> result =
        ChangeEventGeneratorUtils.convertEntityGlossaryTermChangeEvents(
            TEST_FIELD_PATH,
            Urn.createFromString(TEST_PARENT_URN),
            List.of(glossaryTermChangeEvent));

    assertEquals(result.size(), 1);

    ChangeEvent schemaFieldEvent = result.get(0);
    assertEquals(schemaFieldEvent.getClass(), SchemaFieldGlossaryTermChangeEvent.class);

    SchemaFieldGlossaryTermChangeEvent schemaFieldGlossaryTermEvent =
        (SchemaFieldGlossaryTermChangeEvent) schemaFieldEvent;
    assertNotNull(schemaFieldGlossaryTermEvent.getParameters());

    Map<String, Object> eventParameters = schemaFieldGlossaryTermEvent.getParameters();
    assertEquals(eventParameters.get("fieldPath"), TEST_FIELD_PATH);
    assertEquals(eventParameters.get("parentUrn"), TEST_PARENT_URN);
    assertEquals(eventParameters.get("termUrn"), TEST_GLOSSARY_TERM_URN);
    assertEquals(eventParameters.get("context"), "{}");

    assertEquals(schemaFieldGlossaryTermEvent.getOperation(), ChangeOperation.REMOVE);
  }

  @Test
  public void testConvertMultipleEntityGlossaryTermChangeEvents() throws Exception {
    AuditStamp auditStamp = getTestAuditStamp();

    // Create multiple GlossaryTermChangeEvents
    GlossaryTermChangeEvent addEvent =
        GlossaryTermChangeEvent.entityGlossaryTermChangeEventBuilder()
            .entityUrn(TEST_ENTITY_URN)
            .category(ChangeCategory.GLOSSARY_TERM)
            .operation(ChangeOperation.ADD)
            .modifier(TEST_GLOSSARY_TERM_URN)
            .glossaryTermAssociation(
                new GlossaryTermAssociation()
                    .setUrn(GlossaryTermUrn.createFromString(TEST_GLOSSARY_TERM_URN))
                    .setContext(TEST_CONTEXT))
            .auditStamp(auditStamp)
            .semVerChange(SemanticChangeType.MINOR)
            .description("Test glossary term added")
            .build();

    GlossaryTermChangeEvent removeEvent =
        GlossaryTermChangeEvent.entityGlossaryTermChangeEventBuilder()
            .entityUrn(TEST_ENTITY_URN)
            .category(ChangeCategory.GLOSSARY_TERM)
            .operation(ChangeOperation.REMOVE)
            .modifier("urn:li:glossaryTerm:Test.Term2")
            .glossaryTermAssociation(
                new GlossaryTermAssociation()
                    .setUrn(GlossaryTermUrn.createFromString("urn:li:glossaryTerm:Test.Term2")))
            .auditStamp(auditStamp)
            .semVerChange(SemanticChangeType.MINOR)
            .description("Test glossary term removed")
            .build();

    List<ChangeEvent> result =
        ChangeEventGeneratorUtils.convertEntityGlossaryTermChangeEvents(
            TEST_FIELD_PATH, Urn.createFromString(TEST_PARENT_URN), List.of(addEvent, removeEvent));

    assertEquals(result.size(), 2);

    // Verify both events are converted correctly
    for (ChangeEvent event : result) {
      assertEquals(event.getClass(), SchemaFieldGlossaryTermChangeEvent.class);
      SchemaFieldGlossaryTermChangeEvent schemaFieldEvent =
          (SchemaFieldGlossaryTermChangeEvent) event;

      Map<String, Object> parameters = schemaFieldEvent.getParameters();
      assertEquals(parameters.get("fieldPath"), TEST_FIELD_PATH);
      assertEquals(parameters.get("parentUrn"), TEST_PARENT_URN);
      assertNotNull(parameters.get("termUrn"));
      assertNotNull(parameters.get("context"));
    }
  }

  @Test
  public void testConvertEmptyList() throws Exception {
    List<ChangeEvent> result =
        ChangeEventGeneratorUtils.convertEntityGlossaryTermChangeEvents(
            TEST_FIELD_PATH, Urn.createFromString(TEST_PARENT_URN), List.of());

    assertEquals(result.size(), 0);
  }

  @Test
  public void testConvertFilteringNonGlossaryTermEvents() throws Exception {
    AuditStamp auditStamp = getTestAuditStamp();

    // Create a mix of events, including non-GlossaryTermChangeEvent
    TagChangeEvent tagChangeEvent =
        TagChangeEvent.entityTagChangeEventBuilder()
            .entityUrn(TEST_ENTITY_URN)
            .category(ChangeCategory.TAG)
            .operation(ChangeOperation.ADD)
            .modifier(TEST_TAG_URN)
            .tagAssociation(
                new TagAssociation()
                    .setTag(TagUrn.createFromString(TEST_TAG_URN))
                    .setContext(TEST_CONTEXT))
            .auditStamp(auditStamp)
            .semVerChange(SemanticChangeType.MINOR)
            .description("Test tag added")
            .build();

    GlossaryTermChangeEvent glossaryTermChangeEvent =
        GlossaryTermChangeEvent.entityGlossaryTermChangeEventBuilder()
            .entityUrn(TEST_ENTITY_URN)
            .category(ChangeCategory.GLOSSARY_TERM)
            .operation(ChangeOperation.ADD)
            .modifier(TEST_GLOSSARY_TERM_URN)
            .glossaryTermAssociation(
                new GlossaryTermAssociation()
                    .setUrn(GlossaryTermUrn.createFromString(TEST_GLOSSARY_TERM_URN))
                    .setContext(TEST_CONTEXT))
            .auditStamp(auditStamp)
            .semVerChange(SemanticChangeType.MINOR)
            .description("Test glossary term added")
            .build();

    List<ChangeEvent> result =
        ChangeEventGeneratorUtils.convertEntityGlossaryTermChangeEvents(
            TEST_FIELD_PATH,
            Urn.createFromString(TEST_PARENT_URN),
            List.of(tagChangeEvent, glossaryTermChangeEvent));

    // Should only include the GlossaryTermChangeEvent, not the TagChangeEvent
    assertEquals(result.size(), 1);
    assertEquals(result.get(0).getClass(), SchemaFieldGlossaryTermChangeEvent.class);
  }
}
