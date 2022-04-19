package com.linkedin.metadata.kafka.hook.event;

import com.datahub.authentication.Authentication;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.GlobalTags;
import com.linkedin.common.GlossaryTerms;
import com.linkedin.common.Ownership;
import com.linkedin.common.Status;
import com.linkedin.common.TagAssociation;
import com.linkedin.common.TagAssociationArray;
import com.linkedin.common.urn.TagUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.DataMap;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.entity.client.RestliEntityClient;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.timeline.data.ChangeCategory;
import com.linkedin.metadata.timeline.data.ChangeOperation;
import com.linkedin.metadata.timeline.differ.AspectDifferRegistry;
import com.linkedin.metadata.timeline.differ.EntityKeyDiffer;
import com.linkedin.metadata.timeline.differ.GlobalTagsDiffer;
import com.linkedin.metadata.timeline.differ.GlossaryTermsDiffer;
import com.linkedin.metadata.timeline.differ.OwnershipDiffer;
import com.linkedin.metadata.timeline.differ.SingleDomainDiffer;
import com.linkedin.metadata.timeline.differ.StatusDiffer;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeLog;
import com.linkedin.mxe.PlatformEvent;
import com.linkedin.mxe.PlatformEventHeader;
import com.linkedin.platform.event.v1.EntityChangeEvent;
import com.linkedin.platform.event.v1.Parameters;
import java.util.Map;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;

import org.testng.annotations.Test;

import static com.linkedin.metadata.Constants.*;


/**
 * Tests the {@link EntityChangeEventGeneratorHook}.
 *
 * TODO: Include Schema Field Tests, description update tests.
 */
public class EntityChangeEventGeneratorHookTest {

  private static final String TEST_DATASET_URN = "urn:li:dataset:(urn:li:dataPlatform:kafka,SampleDataset,PROD)";
  private static final String TEST_ACTOR_URN = "urn:li:corpuser:test";

  private RestliEntityClient _mockClient;
  private EntityChangeEventGeneratorHook _entityChangeEventHook;

  @BeforeMethod
  public void setupTest() {
    AspectDifferRegistry differRegistry = createAspectDifferRegistry();
    Authentication mockAuthentication = Mockito.mock(Authentication.class);
    _mockClient = Mockito.mock(RestliEntityClient.class);
    _entityChangeEventHook = new EntityChangeEventGeneratorHook(
        differRegistry,
        _mockClient,
        mockAuthentication,
        createMockEntityRegistry());
  }

  @Test
  public void testInvokeEntityAddTagChange() throws Exception {
    MetadataChangeLog event = new MetadataChangeLog();
    event.setEntityType(DATASET_ENTITY_NAME);
    event.setAspectName(GLOBAL_TAGS_ASPECT_NAME);
    event.setChangeType(ChangeType.UPSERT);
    final GlobalTags newTags = new GlobalTags();
    final TagUrn newTagUrn = new TagUrn("Test");
    final long eventTime = 123L;
    newTags.setTags(new TagAssociationArray(
        ImmutableList.of(new TagAssociation()
            .setTag(newTagUrn)
        )
    ));
    event.setAspect(GenericRecordUtils.serializeAspect(newTags));
    event.setEntityUrn(Urn.createFromString(TEST_DATASET_URN));
    event.setEntityType(DATASET_ENTITY_NAME);
    event.setCreated(new AuditStamp().setActor(Urn.createFromString(TEST_ACTOR_URN)).setTime(eventTime));

    // No previous tags aspect.
    _entityChangeEventHook.invoke(
        event
    );

    // Create Platform Event
    PlatformEvent platformEvent = createChangeEvent(
        DATASET_ENTITY_NAME,
        Urn.createFromString(TEST_DATASET_URN),
        ChangeCategory.TAG,
        ChangeOperation.ADD,
        newTagUrn.toString(),
        ImmutableMap.of(
            "tagUrn", newTagUrn.toString()
        ),
        Urn.createFromString(TEST_ACTOR_URN),
        eventTime
    );

    // Verify event has been emitted.
    Mockito.verify(_mockClient, Mockito.times(1))
        .producePlatformEvent(
            Mockito.eq(CHANGE_EVENT_PLATFORM_EVENT_NAME),
            Mockito.anyString(),
            Mockito.eq(platformEvent),
            Mockito.any(Authentication.class)
        );

    Mockito.verifyNoMoreInteractions(_mockClient);
  }

  @Test
  public void testInvokeEntityRemoveTagChange() throws Exception {
    MetadataChangeLog event = new MetadataChangeLog();
    event.setEntityType(DATASET_ENTITY_NAME);
    event.setAspectName(GLOBAL_TAGS_ASPECT_NAME);
    event.setChangeType(ChangeType.UPSERT);
    final GlobalTags existingTags = new GlobalTags();
    final TagUrn newTagUrn = new TagUrn("Test");
    final long eventTime = 123L;
    existingTags.setTags(new TagAssociationArray(
        ImmutableList.of(new TagAssociation()
            .setTag(newTagUrn)
        )
    ));
    event.setPreviousAspectValue(GenericRecordUtils.serializeAspect(existingTags));
    event.setEntityUrn(Urn.createFromString(TEST_DATASET_URN));
    event.setEntityType(DATASET_ENTITY_NAME);
    event.setCreated(new AuditStamp().setActor(Urn.createFromString(TEST_ACTOR_URN)).setTime(eventTime));

    // No previous tags aspect.
    _entityChangeEventHook.invoke(
        event
    );

    // Create Platform Event
    PlatformEvent platformEvent = createChangeEvent(
        DATASET_ENTITY_NAME,
        Urn.createFromString(TEST_DATASET_URN),
        ChangeCategory.TAG,
        ChangeOperation.REMOVE,
        newTagUrn.toString(),
        ImmutableMap.of(
            "tagUrn", newTagUrn.toString()
        ),
        Urn.createFromString(TEST_ACTOR_URN),
        eventTime
    );

    // Verify event has been emitted.
    Mockito.verify(_mockClient, Mockito.times(1))
        .producePlatformEvent(
            Mockito.eq(CHANGE_EVENT_PLATFORM_EVENT_NAME),
            Mockito.anyString(),
            Mockito.eq(platformEvent),
            Mockito.any(Authentication.class)
        );

    Mockito.verifyNoMoreInteractions(_mockClient);
  }

  @Test
  public void testInvokeEntityTermChange() throws Exception {

  }

  @Test
  public void testInvokeEntityDomainChange() throws Exception {

  }

  @Test
  public void testInvokeEntityOwnerChange() throws Exception {

  }

  @Test
  public void testInvokeEntityTermDeprecation() throws Exception {

  }

  @Test
  public void testInvokeEntityCreate() throws Exception {

  }

  @Test
  public void testInvokeEntityHardDelete() throws Exception {

  }

  @Test
  public void testInvokeEntitySoftDelete() throws Exception {

  }


  @Test
  public void testInvokeIneligibleAspect() throws Exception {

  }

  private PlatformEvent createChangeEvent(
      String entityType,
      Urn entityUrn,
      ChangeCategory category,
      ChangeOperation operation,
      String modifier,
      Map<String, Object> parameters,
      Urn actor,
      long timestamp) throws Exception {
    final EntityChangeEvent changeEvent = new EntityChangeEvent();
    changeEvent.setEntityType(entityType);
    changeEvent.setEntityUrn(entityUrn);
    changeEvent.setCategory(category.name());
    changeEvent.setOperation(operation.name());
    changeEvent.setModifier(modifier);
    changeEvent.setAuditStamp(new AuditStamp()
      .setActor(actor)
      .setTime(timestamp)
    );
    changeEvent.setVersion(0);
    changeEvent.setParameters(new Parameters(new DataMap(parameters)));
    final PlatformEvent platformEvent = new PlatformEvent();
    platformEvent.setName(CHANGE_EVENT_PLATFORM_EVENT_NAME);
    platformEvent.setHeader(new PlatformEventHeader().setTimestampMillis(timestamp));
    platformEvent.setPayload(GenericRecordUtils.serializePayload(
        changeEvent
    ));
    return platformEvent;
  }

  private AspectDifferRegistry createAspectDifferRegistry() {
    final AspectDifferRegistry registry = new AspectDifferRegistry();
    registry.register(GLOBAL_TAGS_ASPECT_NAME, new GlobalTagsDiffer());
    registry.register(GLOSSARY_TERMS_ASPECT_NAME, new GlossaryTermsDiffer());
    registry.register(DOMAINS_ASPECT_NAME, new SingleDomainDiffer());
    registry.register(OWNERSHIP_ASPECT_NAME, new OwnershipDiffer());
    registry.register(STATUS_ASPECT_NAME, new StatusDiffer());

    // TODO Add Dataset Schema Field related differs.

    // Entity Lifecycle Differs
    registry.register(DATASET_KEY_ASPECT_NAME, new EntityKeyDiffer<>());
    return registry;
  }

  private EntityRegistry createMockEntityRegistry() {
    EntityRegistry registry = Mockito.mock(EntityRegistry.class);
    // Build Dataset Entity Spec
    EntitySpec datasetSpec = Mockito.mock(EntitySpec.class);

    AspectSpec mockTags = createMockAspectSpec(GlobalTags.class);
    Mockito.when(datasetSpec.getAspectSpec(Mockito.eq(GLOBAL_TAGS_ASPECT_NAME))).thenReturn(mockTags);

    AspectSpec mockTerms = createMockAspectSpec(GlossaryTerms.class);
    Mockito.when(datasetSpec.getAspectSpec(Mockito.eq(GLOSSARY_TERMS_ASPECT_NAME))).thenReturn(mockTerms);

    AspectSpec mockOwners = createMockAspectSpec(Ownership.class);
    Mockito.when(datasetSpec.getAspectSpec(Mockito.eq(OWNERSHIP_ASPECT_NAME))).thenReturn(mockOwners);

    AspectSpec mockStatus = createMockAspectSpec(Status.class);
    Mockito.when(datasetSpec.getAspectSpec(Mockito.eq(STATUS_ASPECT_NAME))).thenReturn(mockStatus);

    Mockito.when(registry.getEntitySpec(Mockito.eq(DATASET_ENTITY_NAME)))
        .thenReturn(datasetSpec);
    return registry;
  }

  private <T extends RecordTemplate> AspectSpec createMockAspectSpec(Class<T> clazz) {
    AspectSpec mockSpec = Mockito.mock(AspectSpec.class);
    Mockito.when(mockSpec.getDataTemplateClass()).thenReturn((Class<RecordTemplate>) clazz);
    return mockSpec;
  }
}