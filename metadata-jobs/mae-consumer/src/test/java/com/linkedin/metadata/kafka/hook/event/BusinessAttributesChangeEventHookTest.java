package com.linkedin.metadata.kafka.hook.event;

import static com.linkedin.metadata.Constants.BUSINESS_ATTRIBUTE_ASPECT;
import static com.linkedin.metadata.Constants.CHANGE_EVENT_PLATFORM_EVENT_NAME;
import static com.linkedin.metadata.Constants.SCHEMA_FIELD_ENTITY_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.testng.Assert.assertEquals;

import com.google.common.collect.ImmutableMap;
import com.linkedin.businessattribute.BusinessAttributeAssociation;
import com.linkedin.businessattribute.BusinessAttributes;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.BusinessAttributeUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.gms.factory.timeline.eventgenerator.EntityChangeEventGeneratorRegistryFactory;
import com.linkedin.metadata.event.EventProducer;
import com.linkedin.metadata.timeline.eventgenerator.EntityChangeEventGeneratorRegistry;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeLog;
import com.linkedin.mxe.PlatformEvent;
import com.linkedin.platform.event.v1.EntityChangeEvent;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.config.YamlPropertiesFactoryBean;
import org.springframework.context.ApplicationContext;
import org.springframework.core.io.ClassPathResource;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.util.PropertyPlaceholderHelper;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Drives a {@code businessAttributes} change on a schema field through {@link
 * PlatformEventGeneratorHook}, using the generator registry built by the production {@link
 * EntityChangeEventGeneratorRegistryFactory} and the real entity registry. This covers the
 * registration key, the hook's supported-aspect filter, aspect deserialization and the generator.
 *
 * <p>{@code businessAttributes} is only attached to {@code schemaField}, and the default {@code
 * entityChangeEvents.entityExclusions} excludes {@code schemaField}, so these events are only
 * emitted when that default is overridden ({@code ECE_ENTITY_EXCLUSIONS}). The emitting tests build
 * the hook without exclusions; {@link #testDefaultExclusionsDropBusinessAttributeChanges()} pins
 * the default behavior.
 */
public class BusinessAttributesChangeEventHookTest {

  private static final Urn DATASET_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,my_db.my_table,PROD)");
  private static final Urn FIELD_URN =
      UrnUtils.getUrn("urn:li:schemaField:(" + DATASET_URN + ",col_a)");
  private static final String ATTRIBUTE_A = "urn:li:businessAttribute:attributeA";
  private static final String ATTRIBUTE_B = "urn:li:businessAttribute:attributeB";
  private static final Urn ACTOR = UrnUtils.getUrn("urn:li:corpuser:test");

  private EventProducer producer;
  private EntityChangeEventGeneratorRegistry registry;
  private PlatformEventGeneratorHook hook;
  private OperationContext opContext;

  /** Exposes the factory's protected bean method. */
  private static class ExposedGeneratorFactory extends EntityChangeEventGeneratorRegistryFactory {
    EntityChangeEventGeneratorRegistry build() {
      return entityChangeEventGeneratorRegistry(
          mock(OperationContext.class), mock(SystemEntityClient.class));
    }
  }

  @BeforeMethod
  public void setUp() {
    ExposedGeneratorFactory factory = new ExposedGeneratorFactory();
    // The factory reads this @Autowired field before building; the bean it returns is unused here.
    ReflectionTestUtils.setField(factory, "applicationContext", mock(ApplicationContext.class));
    producer = mock(EventProducer.class);
    registry = factory.build();
    hook = new PlatformEventGeneratorHook(registry, producer, true);
    opContext = TestOperationContexts.systemContextNoSearchAuthorization();
  }

  @Test
  public void testAddEmitsAdd() throws Exception {
    hook.invoke(opContext, changeLog(null, ATTRIBUTE_A));

    List<EntityChangeEvent> events = emittedEvents(1);
    assertEvent(events.get(0), "ADD", ATTRIBUTE_A);
  }

  @Test
  public void testRemoveEmitsRemove() throws Exception {
    hook.invoke(opContext, changeLog(ATTRIBUTE_A, null));

    List<EntityChangeEvent> events = emittedEvents(1);
    assertEvent(events.get(0), "REMOVE", ATTRIBUTE_A);
  }

  @Test
  public void testReplaceEmitsRemoveThenAdd() throws Exception {
    hook.invoke(opContext, changeLog(ATTRIBUTE_A, ATTRIBUTE_B));

    List<EntityChangeEvent> events = emittedEvents(2);
    assertEvent(events.get(0), "REMOVE", ATTRIBUTE_A);
    assertEvent(events.get(1), "ADD", ATTRIBUTE_B);
  }

  @Test
  public void testUnchangedEmitsNothing() throws Exception {
    hook.invoke(opContext, changeLog(ATTRIBUTE_A, ATTRIBUTE_A));

    verifyNoInteractions(producer);
  }

  @Test
  public void testDefaultExclusionsDropBusinessAttributeChanges() throws Exception {
    PlatformEventGeneratorHook defaultHook =
        new PlatformEventGeneratorHook(
            registry, producer, true, "", defaultEntityExclusions(), List.of(), false);

    defaultHook.invoke(opContext, changeLog(ATTRIBUTE_A, ATTRIBUTE_B));

    // Relationship change events for BusinessAttributeOf are not subject to the exclusion list and
    // are still emitted; only the entity change events are dropped.
    verify(producer, never())
        .producePlatformEvent(
            any(OperationContext.class),
            eq(CHANGE_EVENT_PLATFORM_EVENT_NAME),
            anyString(),
            any(PlatformEvent.class));
  }

  /** The hook's {@code entityChangeEvents.entityExclusions} default, read from application.yaml. */
  private static List<String> defaultEntityExclusions() {
    YamlPropertiesFactoryBean yaml = new YamlPropertiesFactoryBean();
    yaml.setResources(new ClassPathResource("application.yaml"));
    Properties properties = yaml.getObject();
    PropertyPlaceholderHelper helper = new PropertyPlaceholderHelper("${", "}", ":", null, true);
    // Resolve against the yaml alone, so an ECE_ENTITY_EXCLUSIONS set where the test runs is
    // ignored.
    String exclusions =
        helper.replacePlaceholders(
            properties.getProperty("entityChangeEvents.entityExclusions"), properties::getProperty);
    assertEquals(exclusions, SCHEMA_FIELD_ENTITY_NAME);
    return Arrays.asList(exclusions.split(","));
  }

  private static MetadataChangeLog changeLog(String previousAttribute, String newAttribute)
      throws Exception {
    MetadataChangeLog event = new MetadataChangeLog();
    event.setEntityType(SCHEMA_FIELD_ENTITY_NAME);
    event.setEntityUrn(FIELD_URN);
    event.setAspectName(BUSINESS_ATTRIBUTE_ASPECT);
    event.setChangeType(ChangeType.UPSERT);
    event.setCreated(new AuditStamp().setActor(ACTOR).setTime(123L));
    if (previousAttribute != null) {
      event.setPreviousAspectValue(GenericRecordUtils.serializeAspect(aspect(previousAttribute)));
    }
    event.setAspect(GenericRecordUtils.serializeAspect(aspect(newAttribute)));
    return event;
  }

  private static BusinessAttributes aspect(String attributeUrn) throws Exception {
    BusinessAttributes aspect = new BusinessAttributes();
    if (attributeUrn != null) {
      aspect.setBusinessAttribute(
          new BusinessAttributeAssociation()
              .setBusinessAttributeUrn(BusinessAttributeUrn.createFromString(attributeUrn)));
    }
    return aspect;
  }

  private List<EntityChangeEvent> emittedEvents(int expectedCount) throws Exception {
    ArgumentCaptor<PlatformEvent> captor = ArgumentCaptor.forClass(PlatformEvent.class);
    verify(producer, times(expectedCount))
        .producePlatformEvent(
            any(OperationContext.class),
            eq(CHANGE_EVENT_PLATFORM_EVENT_NAME),
            anyString(),
            captor.capture());
    return captor.getAllValues().stream()
        .map(
            platformEvent ->
                GenericRecordUtils.deserializePayload(
                    platformEvent.getPayload().getValue(), EntityChangeEvent.class))
        .collect(Collectors.toList());
  }

  private static void assertEvent(EntityChangeEvent event, String operation, String attributeUrn) {
    assertEquals(event.getEntityType(), SCHEMA_FIELD_ENTITY_NAME);
    assertEquals(event.getEntityUrn(), FIELD_URN);
    assertEquals(event.getCategory(), "BUSINESS_ATTRIBUTE");
    assertEquals(event.getOperation(), operation);
    assertEquals(event.getModifier(), attributeUrn);
    assertEquals(event.getAuditStamp().getActor(), ACTOR);
    Map<String, Object> expectedParameters =
        ImmutableMap.of(
            "businessAttributeUrn", attributeUrn,
            "parentUrn", FIELD_URN.toString(),
            "datasetUrn", DATASET_URN.toString());
    assertEquals(event.getParameters().data(), expectedParameters);
  }
}
