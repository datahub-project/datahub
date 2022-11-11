package com.linkedin.metadata.kafka.hook.event;

import com.datahub.authentication.Authentication;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.linkedin.assertion.AssertionResult;
import com.linkedin.assertion.AssertionResultType;
import com.linkedin.assertion.AssertionRunEvent;
import com.linkedin.assertion.AssertionRunStatus;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.Deprecation;
import com.linkedin.common.FabricType;
import com.linkedin.common.GlobalTags;
import com.linkedin.common.GlossaryTermAssociation;
import com.linkedin.common.GlossaryTermAssociationArray;
import com.linkedin.common.GlossaryTerms;
import com.linkedin.common.Owner;
import com.linkedin.common.OwnerArray;
import com.linkedin.common.Ownership;
import com.linkedin.common.OwnershipType;
import com.linkedin.common.Status;
import com.linkedin.common.TagAssociation;
import com.linkedin.common.TagAssociationArray;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.GlossaryTermUrn;
import com.linkedin.common.urn.TagUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.DataMap;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.dataprocess.DataProcessInstanceRelationships;
import com.linkedin.dataprocess.DataProcessInstanceRunEvent;
import com.linkedin.dataprocess.DataProcessInstanceRunResult;
import com.linkedin.dataprocess.DataProcessRunStatus;
import com.linkedin.dataprocess.RunResultType;
import com.linkedin.dataset.DatasetProperties;
import com.linkedin.domain.Domains;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.entity.client.RestliEntityClient;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.key.DatasetKey;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.timeline.data.ChangeCategory;
import com.linkedin.metadata.timeline.data.ChangeOperation;
import com.linkedin.metadata.timeline.eventgenerator.AssertionRunEventChangeEventGenerator;
import com.linkedin.metadata.timeline.eventgenerator.DataProcessInstanceRunEventChangeEventGenerator;
import com.linkedin.metadata.timeline.eventgenerator.DeprecationChangeEventGenerator;
import com.linkedin.metadata.timeline.eventgenerator.EntityChangeEventGeneratorRegistry;
import com.linkedin.metadata.timeline.eventgenerator.EntityKeyChangeEventGenerator;
import com.linkedin.metadata.timeline.eventgenerator.GlobalTagsChangeEventGenerator;
import com.linkedin.metadata.timeline.eventgenerator.GlossaryTermsChangeEventGenerator;
import com.linkedin.metadata.timeline.eventgenerator.OwnershipChangeEventGenerator;
import com.linkedin.metadata.timeline.eventgenerator.SingleDomainChangeEventGenerator;
import com.linkedin.metadata.timeline.eventgenerator.StatusChangeEventGenerator;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeLog;
import com.linkedin.mxe.PlatformEvent;
import com.linkedin.mxe.PlatformEventHeader;
import com.linkedin.platform.event.v1.EntityChangeEvent;
import com.linkedin.platform.event.v1.Parameters;
import java.net.URISyntaxException;
import java.util.Map;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.linkedin.metadata.Constants.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;


/**
 * Tests the {@link EntityChangeEventGeneratorHook}.
 *
 * TODO: Include Schema Field Tests, description update tests.
 */
public class EntityChangeEventGeneratorHookTest {
  private static final long EVENT_TIME = 123L;

  private static final String TEST_DATASET_URN = "urn:li:dataset:(urn:li:dataPlatform:kafka,SampleDataset,PROD)";
  private static final String TEST_ACTOR_URN = "urn:li:corpuser:test";
  private static final String TEST_ASSERTION_URN = "urn:li:assertion:123";
  private static final String TEST_RUN_ID = "runId";
  private static final String TEST_DATA_PROCESS_INSTANCE_URN = "urn:li:dataProcessInstance:instance";
  private static final String TEST_DATA_PROCESS_INSTANCE_PARENT_URN = "urn:li:dataProcessInstance:parent";
  private static final String TEST_DATA_FLOW_URN = "urn:li:dataFlow:flow";
  private static final String TEST_DATA_JOB_URN = "urn:li:dataJob:job";
  private Urn actorUrn;
  private Authentication _mockAuthentication;

  private RestliEntityClient _mockClient;
  private EntityService _mockEntityService;
  private EntityChangeEventGeneratorHook _entityChangeEventHook;

  @BeforeMethod
  public void setupTest() throws URISyntaxException {
    actorUrn = Urn.createFromString(TEST_ACTOR_URN);
    _mockAuthentication = Mockito.mock(Authentication.class);
    _mockClient = Mockito.mock(RestliEntityClient.class);
    _mockEntityService = Mockito.mock(EntityService.class);
    EntityChangeEventGeneratorRegistry entityChangeEventGeneratorRegistry = createEntityChangeEventGeneratorRegistry();
    _entityChangeEventHook =
        new EntityChangeEventGeneratorHook(entityChangeEventGeneratorRegistry, _mockClient, _mockAuthentication,
            createMockEntityRegistry(), true);
  }

  @Test
  public void testInvokeEntityAddTagChange() throws Exception {
    MetadataChangeLog event = new MetadataChangeLog();
    event.setEntityType(DATASET_ENTITY_NAME);
    event.setAspectName(GLOBAL_TAGS_ASPECT_NAME);
    event.setChangeType(ChangeType.UPSERT);
    final GlobalTags newTags = new GlobalTags();
    final TagUrn newTagUrn = new TagUrn("Test");
    newTags.setTags(new TagAssociationArray(
        ImmutableList.of(new TagAssociation()
            .setTag(newTagUrn)
        )));
    event.setAspect(GenericRecordUtils.serializeAspect(newTags));
    event.setEntityUrn(Urn.createFromString(TEST_DATASET_URN));
    event.setEntityType(DATASET_ENTITY_NAME);
    event.setCreated(new AuditStamp().setActor(actorUrn).setTime(EVENT_TIME));

    // No previous tags aspect.
    _entityChangeEventHook.invoke(event);

    // Create Platform Event
    PlatformEvent platformEvent =
        createChangeEvent(DATASET_ENTITY_NAME, Urn.createFromString(TEST_DATASET_URN), ChangeCategory.TAG,
            ChangeOperation.ADD, newTagUrn.toString(), ImmutableMap.of("tagUrn", newTagUrn.toString()), actorUrn);

    verifyProducePlatformEvent(_mockClient, platformEvent);
  }

  @Test
  public void testInvokeEntityRemoveTagChange() throws Exception {
    MetadataChangeLog event = new MetadataChangeLog();
    event.setEntityType(DATASET_ENTITY_NAME);
    event.setAspectName(GLOBAL_TAGS_ASPECT_NAME);
    event.setChangeType(ChangeType.UPSERT);
    final GlobalTags existingTags = new GlobalTags();
    final TagUrn newTagUrn = new TagUrn("Test");
    existingTags.setTags(new TagAssociationArray(
        ImmutableList.of(new TagAssociation()
            .setTag(newTagUrn)
        )));
    event.setPreviousAspectValue(GenericRecordUtils.serializeAspect(existingTags));
    event.setEntityUrn(Urn.createFromString(TEST_DATASET_URN));
    event.setEntityType(DATASET_ENTITY_NAME);
    event.setCreated(new AuditStamp().setActor(actorUrn).setTime(EVENT_TIME));

    // No previous tags aspect.
    _entityChangeEventHook.invoke(event);

    // Create Platform Event
    PlatformEvent platformEvent =
        createChangeEvent(DATASET_ENTITY_NAME, Urn.createFromString(TEST_DATASET_URN), ChangeCategory.TAG,
            ChangeOperation.REMOVE, newTagUrn.toString(), ImmutableMap.of("tagUrn", newTagUrn.toString()), actorUrn);

    verifyProducePlatformEvent(_mockClient, platformEvent);
  }

  @Test
  public void testInvokeEntityAddTermChange() throws Exception {
    MetadataChangeLog event = new MetadataChangeLog();
    event.setEntityType(DATASET_ENTITY_NAME);
    event.setAspectName(GLOSSARY_TERMS_ASPECT_NAME);
    event.setChangeType(ChangeType.UPSERT);
    final GlossaryTerms newTerms = new GlossaryTerms();
    final GlossaryTermUrn glossaryTermUrn = new GlossaryTermUrn("TestTerm");
    newTerms.setTerms(new GlossaryTermAssociationArray(
        ImmutableList.of(new GlossaryTermAssociation()
            .setUrn(glossaryTermUrn)
        )
    ));
    final GlossaryTerms previousTerms = new GlossaryTerms();
    previousTerms.setTerms(new GlossaryTermAssociationArray());
    event.setAspect(GenericRecordUtils.serializeAspect(newTerms));
    event.setPreviousAspectValue(GenericRecordUtils.serializeAspect(previousTerms));
    event.setEntityUrn(Urn.createFromString(TEST_DATASET_URN));
    event.setEntityType(DATASET_ENTITY_NAME);
    event.setCreated(new AuditStamp().setActor(actorUrn).setTime(EVENT_TIME));

    // No previous tags aspect.
    _entityChangeEventHook.invoke(event);

    // Create Platform Event
    PlatformEvent platformEvent =
        createChangeEvent(DATASET_ENTITY_NAME, Urn.createFromString(TEST_DATASET_URN), ChangeCategory.GLOSSARY_TERM,
            ChangeOperation.ADD, glossaryTermUrn.toString(), ImmutableMap.of("termUrn", glossaryTermUrn.toString()),
            actorUrn);

    verifyProducePlatformEvent(_mockClient, platformEvent);
  }

  @Test
  public void testInvokeEntityRemoveTermChange() throws Exception {
    MetadataChangeLog event = new MetadataChangeLog();
    event.setEntityType(DATASET_ENTITY_NAME);
    event.setAspectName(GLOSSARY_TERMS_ASPECT_NAME);
    event.setChangeType(ChangeType.UPSERT);
    final GlossaryTerms newTerms = new GlossaryTerms();
    newTerms.setTerms(new GlossaryTermAssociationArray());
    final GlossaryTerms previousTerms = new GlossaryTerms();
    final GlossaryTermUrn glossaryTermUrn = new GlossaryTermUrn("TestTerm");
    previousTerms.setTerms(new GlossaryTermAssociationArray(
        ImmutableList.of(new GlossaryTermAssociation()
            .setUrn(glossaryTermUrn)
        )
    ));
    event.setAspect(GenericRecordUtils.serializeAspect(newTerms));
    event.setPreviousAspectValue(GenericRecordUtils.serializeAspect(previousTerms));
    event.setEntityUrn(Urn.createFromString(TEST_DATASET_URN));
    event.setEntityType(DATASET_ENTITY_NAME);
    event.setCreated(new AuditStamp().setActor(actorUrn).setTime(EVENT_TIME));

    // No previous tags aspect.
    _entityChangeEventHook.invoke(event);

    // Create Platform Event
    PlatformEvent platformEvent =
        createChangeEvent(DATASET_ENTITY_NAME, Urn.createFromString(TEST_DATASET_URN), ChangeCategory.GLOSSARY_TERM,
            ChangeOperation.REMOVE, glossaryTermUrn.toString(), ImmutableMap.of("termUrn", glossaryTermUrn.toString()),
            actorUrn);

    verifyProducePlatformEvent(_mockClient, platformEvent);
  }

  @Test
  public void testInvokeEntitySetDomain() throws Exception {
    MetadataChangeLog event = new MetadataChangeLog();
    event.setEntityType(DATASET_ENTITY_NAME);
    event.setAspectName(DOMAINS_ASPECT_NAME);
    event.setChangeType(ChangeType.UPSERT);
    final Domains newDomains = new Domains();
    final Urn domainUrn = Urn.createFromString("urn:li:domain:test");
    newDomains.setDomains(new UrnArray(
        ImmutableList.of(domainUrn)));
    event.setAspect(GenericRecordUtils.serializeAspect(newDomains));
    event.setEntityUrn(Urn.createFromString(TEST_DATASET_URN));
    event.setEntityType(DATASET_ENTITY_NAME);
    event.setCreated(new AuditStamp().setActor(actorUrn).setTime(EVENT_TIME));

    // No previous tags aspect.
    _entityChangeEventHook.invoke(event);

    // Create Platform Event
    PlatformEvent platformEvent =
        createChangeEvent(DATASET_ENTITY_NAME, Urn.createFromString(TEST_DATASET_URN), ChangeCategory.DOMAIN,
            ChangeOperation.ADD, domainUrn.toString(), ImmutableMap.of("domainUrn", domainUrn.toString()), actorUrn);

    verifyProducePlatformEvent(_mockClient, platformEvent);
  }

  @Test
  public void testInvokeEntityUnsetDomain() throws Exception {
    MetadataChangeLog event = new MetadataChangeLog();
    event.setEntityType(DATASET_ENTITY_NAME);
    event.setAspectName(DOMAINS_ASPECT_NAME);
    event.setChangeType(ChangeType.UPSERT);
    final Domains previousDomains = new Domains();
    final Urn domainUrn = Urn.createFromString("urn:li:domain:test");
    previousDomains.setDomains(new UrnArray(
        ImmutableList.of(domainUrn)));
    event.setPreviousAspectValue(GenericRecordUtils.serializeAspect(previousDomains));
    event.setEntityUrn(Urn.createFromString(TEST_DATASET_URN));
    event.setEntityType(DATASET_ENTITY_NAME);
    event.setCreated(new AuditStamp().setActor(actorUrn).setTime(EVENT_TIME));

    // No previous tags aspect.
    _entityChangeEventHook.invoke(event);

    // Create Platform Event
    PlatformEvent platformEvent =
        createChangeEvent(DATASET_ENTITY_NAME, Urn.createFromString(TEST_DATASET_URN), ChangeCategory.DOMAIN,
            ChangeOperation.REMOVE, domainUrn.toString(), ImmutableMap.of("domainUrn", domainUrn.toString()), actorUrn);

    verifyProducePlatformEvent(_mockClient, platformEvent);
  }

  @Test
  public void testInvokeEntityOwnerChange() throws Exception {
    MetadataChangeLog event = new MetadataChangeLog();
    event.setEntityType(DATASET_ENTITY_NAME);
    event.setAspectName(OWNERSHIP_ASPECT_NAME);
    event.setChangeType(ChangeType.UPSERT);
    final Ownership newOwners = new Ownership();
    final Urn ownerUrn1 = Urn.createFromString("urn:li:corpuser:test1");
    final Urn ownerUrn2 = Urn.createFromString("urn:li:corpuser:test2");
    newOwners.setOwners(new OwnerArray(
        ImmutableList.of(
            new Owner().setOwner(ownerUrn1).setType(OwnershipType.TECHNICAL_OWNER),
            new Owner().setOwner(ownerUrn2).setType(OwnershipType.BUSINESS_OWNER)
        )
    ));
    final Ownership prevOwners = new Ownership();
    prevOwners.setOwners(new OwnerArray());
    event.setAspect(GenericRecordUtils.serializeAspect(newOwners));
    event.setPreviousAspectValue(GenericRecordUtils.serializeAspect(prevOwners));
    event.setEntityUrn(Urn.createFromString(TEST_DATASET_URN));
    event.setEntityType(DATASET_ENTITY_NAME);
    event.setCreated(new AuditStamp().setActor(actorUrn).setTime(EVENT_TIME));

    // No previous tags aspect.
    _entityChangeEventHook.invoke(event);

    // Create Platform Event
    PlatformEvent platformEvent1 =
        createChangeEvent(DATASET_ENTITY_NAME, Urn.createFromString(TEST_DATASET_URN), ChangeCategory.OWNER,
            ChangeOperation.ADD, ownerUrn1.toString(),
            ImmutableMap.of("ownerUrn", ownerUrn1.toString(), "ownerType", OwnershipType.TECHNICAL_OWNER.toString()),
            actorUrn);
    verifyProducePlatformEvent(_mockClient, platformEvent1, false);

    PlatformEvent platformEvent2 =
        createChangeEvent(DATASET_ENTITY_NAME, Urn.createFromString(TEST_DATASET_URN), ChangeCategory.OWNER,
            ChangeOperation.ADD, ownerUrn2.toString(),
            ImmutableMap.of("ownerUrn", ownerUrn2.toString(), "ownerType", OwnershipType.BUSINESS_OWNER.toString()),
            actorUrn);
    verifyProducePlatformEvent(_mockClient, platformEvent2, true);
  }

  @Test
  public void testInvokeEntityTermDeprecation() throws Exception {
    MetadataChangeLog event = new MetadataChangeLog();
    event.setEntityType(DATASET_ENTITY_NAME);
    event.setAspectName(DEPRECATION_ASPECT_NAME);
    event.setChangeType(ChangeType.UPSERT);

    Deprecation newDeprecation = new Deprecation();
    newDeprecation.setDeprecated(true);
    newDeprecation.setNote("Test Note");
    newDeprecation.setActor(actorUrn);

    event.setAspect(GenericRecordUtils.serializeAspect(newDeprecation));
    event.setEntityUrn(Urn.createFromString(TEST_DATASET_URN));
    event.setEntityType(DATASET_ENTITY_NAME);
    event.setCreated(new AuditStamp().setActor(actorUrn).setTime(EVENT_TIME));

    // No previous tags aspect.
    _entityChangeEventHook.invoke(event);

    // Create Platform Event
    PlatformEvent platformEvent =
        createChangeEvent(DATASET_ENTITY_NAME, Urn.createFromString(TEST_DATASET_URN), ChangeCategory.DEPRECATION,
            ChangeOperation.MODIFY, null, ImmutableMap.of("status", "DEPRECATED"), actorUrn);

    verifyProducePlatformEvent(_mockClient, platformEvent);
  }

  @Test
  public void testInvokeEntityCreate() throws Exception {
    MetadataChangeLog event = new MetadataChangeLog();
    event.setEntityType(DATASET_ENTITY_NAME);
    event.setAspectName(DATASET_KEY_ASPECT_NAME);
    event.setChangeType(ChangeType.UPSERT);

    DatasetKey newDatasetKey = new DatasetKey();
    newDatasetKey.setName("TestName");
    newDatasetKey.setOrigin(FabricType.PROD);
    newDatasetKey.setPlatform(Urn.createFromString("urn:li:dataPlatform:hive"));

    event.setAspect(GenericRecordUtils.serializeAspect(newDatasetKey));
    event.setEntityUrn(Urn.createFromString(TEST_DATASET_URN));
    event.setEntityType(DATASET_ENTITY_NAME);
    event.setCreated(new AuditStamp().setActor(actorUrn).setTime(EVENT_TIME));

    // No previous tags aspect.
    _entityChangeEventHook.invoke(event);

    // Create Platform Event
    PlatformEvent platformEvent =
        createChangeEvent(DATASET_ENTITY_NAME, Urn.createFromString(TEST_DATASET_URN), ChangeCategory.LIFECYCLE,
            ChangeOperation.CREATE, null, null, actorUrn);

    verifyProducePlatformEvent(_mockClient, platformEvent);
  }

  @Test
  public void testInvokeEntityHardDelete() throws Exception {
    MetadataChangeLog event = new MetadataChangeLog();
    event.setEntityType(DATASET_ENTITY_NAME);
    event.setAspectName(DATASET_KEY_ASPECT_NAME);
    event.setChangeType(ChangeType.DELETE);

    DatasetKey newDatasetKey = new DatasetKey();
    newDatasetKey.setName("TestName");
    newDatasetKey.setOrigin(FabricType.PROD);
    newDatasetKey.setPlatform(Urn.createFromString("urn:li:dataPlatform:hive"));

    event.setPreviousAspectValue(GenericRecordUtils.serializeAspect(newDatasetKey));
    event.setEntityUrn(Urn.createFromString(TEST_DATASET_URN));
    event.setEntityType(DATASET_ENTITY_NAME);
    event.setCreated(new AuditStamp().setActor(actorUrn).setTime(EVENT_TIME));

    // No previous tags aspect.
    _entityChangeEventHook.invoke(event);

    // Create Platform Event
    PlatformEvent platformEvent =
        createChangeEvent(DATASET_ENTITY_NAME, Urn.createFromString(TEST_DATASET_URN), ChangeCategory.LIFECYCLE,
            ChangeOperation.HARD_DELETE, null, null, actorUrn);

    verifyProducePlatformEvent(_mockClient, platformEvent);
  }

  @Test
  public void testInvokeEntitySoftDelete() throws Exception {
    MetadataChangeLog event = new MetadataChangeLog();
    event.setEntityType(DATASET_ENTITY_NAME);
    event.setAspectName(STATUS_ASPECT_NAME);
    event.setChangeType(ChangeType.UPSERT);

    Status newStatus = new Status();
    newStatus.setRemoved(true);

    event.setAspect(GenericRecordUtils.serializeAspect(newStatus));
    event.setEntityUrn(Urn.createFromString(TEST_DATASET_URN));
    event.setEntityType(DATASET_ENTITY_NAME);
    event.setCreated(new AuditStamp().setActor(actorUrn).setTime(EVENT_TIME));

    // No previous tags aspect.
    _entityChangeEventHook.invoke(event);

    // Create Platform Event
    PlatformEvent platformEvent =
        createChangeEvent(DATASET_ENTITY_NAME, Urn.createFromString(TEST_DATASET_URN), ChangeCategory.LIFECYCLE,
            ChangeOperation.SOFT_DELETE, null, null, actorUrn);

    verifyProducePlatformEvent(_mockClient, platformEvent);
  }

  @Test
  public void testInvokeAssertionRunEventCreate() throws Exception {
    Urn assertionUrn = Urn.createFromString(TEST_ASSERTION_URN);

    MetadataChangeLog event = new MetadataChangeLog();
    event.setEntityType(ASSERTION_ENTITY_NAME);
    event.setAspectName(ASSERTION_RUN_EVENT_ASPECT_NAME);
    event.setChangeType(ChangeType.UPSERT);

    AssertionRunEvent assertionRunEvent = new AssertionRunEvent();
    assertionRunEvent.setAssertionUrn(assertionUrn);
    assertionRunEvent.setStatus(AssertionRunStatus.COMPLETE);
    assertionRunEvent.setResult(new AssertionResult().setType(AssertionResultType.SUCCESS));
    assertionRunEvent.setRunId(TEST_RUN_ID);
    assertionRunEvent.setAsserteeUrn(Urn.createFromString(TEST_DATASET_URN));

    event.setAspect(GenericRecordUtils.serializeAspect(assertionRunEvent));
    event.setEntityUrn(assertionUrn);
    event.setEntityType(ASSERTION_ENTITY_NAME);
    event.setCreated(new AuditStamp().setActor(actorUrn).setTime(EVENT_TIME));

    _entityChangeEventHook.invoke(event);

    Map<String, Object> paramsMap =
        ImmutableMap.of(
            RUN_RESULT_KEY, ASSERTION_RUN_EVENT_STATUS_COMPLETE,
            RUN_ID_KEY, TEST_RUN_ID,
            ASSERTEE_URN_KEY, TEST_DATASET_URN,
            ASSERTION_RESULT_KEY, AssertionResultType.SUCCESS.toString());

    // Create Platform Event
    PlatformEvent platformEvent =
        createChangeEvent(ASSERTION_ENTITY_NAME, assertionUrn, ChangeCategory.RUN, ChangeOperation.COMPLETED, null,
            paramsMap, actorUrn);

    verifyProducePlatformEvent(_mockClient, platformEvent);
  }

  @Test
  public void testInvokeDataProcessInstanceRunEventStart() throws Exception {
    Urn dataProcessInstanceUrn = Urn.createFromString(TEST_DATA_PROCESS_INSTANCE_URN);

    MetadataChangeLog event = new MetadataChangeLog();
    event.setEntityType(DATA_PROCESS_INSTANCE_ENTITY_NAME);
    event.setEntityUrn(dataProcessInstanceUrn);
    event.setAspectName(DATA_PROCESS_INSTANCE_RUN_EVENT_ASPECT_NAME);
    event.setChangeType(ChangeType.UPSERT);

    DataProcessInstanceRunEvent dataProcessInstanceRunEvent =
        new DataProcessInstanceRunEvent().setStatus(DataProcessRunStatus.STARTED).setAttempt(1);

    event.setAspect(GenericRecordUtils.serializeAspect(dataProcessInstanceRunEvent));
    event.setCreated(new AuditStamp().setActor(actorUrn).setTime(EVENT_TIME));

    DataProcessInstanceRelationships relationships =
        new DataProcessInstanceRelationships().setParentInstance(
                Urn.createFromString(TEST_DATA_PROCESS_INSTANCE_PARENT_URN))
            .setParentTemplate(Urn.createFromString(TEST_DATA_JOB_URN));

    final EntityResponse entityResponse =
        buildEntityResponse(ImmutableMap.of(DATA_PROCESS_INSTANCE_RELATIONSHIPS_ASPECT_NAME, relationships));

    Mockito.when(_mockClient.getV2(eq(DATA_PROCESS_INSTANCE_ENTITY_NAME), eq(dataProcessInstanceUrn),
        any(), eq(_mockAuthentication))).thenReturn(entityResponse);

    _entityChangeEventHook.invoke(event);

    Map<String, Object> parameters =
        ImmutableMap.of(ATTEMPT_KEY, 1, PARENT_INSTANCE_URN_KEY, TEST_DATA_PROCESS_INSTANCE_PARENT_URN,
            DATA_JOB_URN_KEY, TEST_DATA_JOB_URN);

    // Create Platform Event
    PlatformEvent platformEvent =
        createChangeEvent(DATA_PROCESS_INSTANCE_ENTITY_NAME, dataProcessInstanceUrn, ChangeCategory.RUN,
            ChangeOperation.STARTED, null, parameters, actorUrn);

    verifyProducePlatformEvent(_mockClient, platformEvent, false);
  }

  @Test
  public void testInvokeDataProcessInstanceRunEventComplete() throws Exception {
    Urn dataProcessInstanceUrn = Urn.createFromString(TEST_DATA_PROCESS_INSTANCE_URN);

    MetadataChangeLog event = new MetadataChangeLog();
    event.setEntityType(DATA_PROCESS_INSTANCE_ENTITY_NAME);
    event.setEntityUrn(dataProcessInstanceUrn);
    event.setAspectName(DATA_PROCESS_INSTANCE_RUN_EVENT_ASPECT_NAME);
    event.setChangeType(ChangeType.UPSERT);

    DataProcessInstanceRunEvent dataProcessInstanceRunEvent =
        new DataProcessInstanceRunEvent().setStatus(DataProcessRunStatus.COMPLETE)
            .setAttempt(1)
            .setResult(new DataProcessInstanceRunResult().setType(RunResultType.SUCCESS));

    event.setAspect(GenericRecordUtils.serializeAspect(dataProcessInstanceRunEvent));
    event.setCreated(new AuditStamp().setActor(actorUrn).setTime(EVENT_TIME));

    DataProcessInstanceRelationships relationships =
        new DataProcessInstanceRelationships().setParentInstance(
                Urn.createFromString(TEST_DATA_PROCESS_INSTANCE_PARENT_URN))
            .setParentTemplate(Urn.createFromString(TEST_DATA_FLOW_URN));
    final EntityResponse entityResponse =
        buildEntityResponse(ImmutableMap.of(DATA_PROCESS_INSTANCE_RELATIONSHIPS_ASPECT_NAME, relationships));

    Mockito.when(_mockClient.getV2(eq(DATA_PROCESS_INSTANCE_ENTITY_NAME), eq(dataProcessInstanceUrn),
        any(), eq(_mockAuthentication))).thenReturn(entityResponse);

    _entityChangeEventHook.invoke(event);

    Map<String, Object> parameters =
        ImmutableMap.of(ATTEMPT_KEY, 1, RUN_RESULT_KEY, RunResultType.SUCCESS.toString(), PARENT_INSTANCE_URN_KEY,
            TEST_DATA_PROCESS_INSTANCE_PARENT_URN, DATA_FLOW_URN_KEY, TEST_DATA_FLOW_URN);

    // Create Platform Event
    PlatformEvent platformEvent =
        createChangeEvent(DATA_PROCESS_INSTANCE_ENTITY_NAME, dataProcessInstanceUrn, ChangeCategory.RUN,
            ChangeOperation.COMPLETED, null, parameters, actorUrn);

    verifyProducePlatformEvent(_mockClient, platformEvent, false);
  }

  @Test
  public void testInvokeIneligibleAspect() throws Exception {
    MetadataChangeLog event = new MetadataChangeLog();
    event.setEntityType(DATASET_ENTITY_NAME);
    event.setAspectName(DATAHUB_POLICY_INFO_ASPECT_NAME);
    event.setChangeType(ChangeType.UPSERT);

    DatasetProperties props = new DatasetProperties();
    props.setName("Test name");

    event.setAspect(GenericRecordUtils.serializeAspect(props));
    event.setEntityUrn(Urn.createFromString(TEST_DATASET_URN));
    event.setEntityType(DATASET_ENTITY_NAME);
    event.setCreated(new AuditStamp().setActor(actorUrn).setTime(EVENT_TIME));

    // No previous tags aspect.
    _entityChangeEventHook.invoke(event);
    // Verify 0 interactions
    Mockito.verifyNoMoreInteractions(_mockClient);
  }

  private PlatformEvent createChangeEvent(String entityType, Urn entityUrn, ChangeCategory category,
      ChangeOperation operation, String modifier, Map<String, Object> parameters, Urn actor) {
    final EntityChangeEvent changeEvent = new EntityChangeEvent();
    changeEvent.setEntityType(entityType);
    changeEvent.setEntityUrn(entityUrn);
    changeEvent.setCategory(category.name());
    changeEvent.setOperation(operation.name());
    if (modifier != null) {
      changeEvent.setModifier(modifier);
    }
    changeEvent.setAuditStamp(new AuditStamp().setActor(actor).setTime(EntityChangeEventGeneratorHookTest.EVENT_TIME));
    changeEvent.setVersion(0);
    if (parameters != null) {
      changeEvent.setParameters(new Parameters(new DataMap(parameters)));
    }
    final PlatformEvent platformEvent = new PlatformEvent();
    platformEvent.setName(CHANGE_EVENT_PLATFORM_EVENT_NAME);
    platformEvent.setHeader(
        new PlatformEventHeader().setTimestampMillis(EntityChangeEventGeneratorHookTest.EVENT_TIME));
    platformEvent.setPayload(GenericRecordUtils.serializePayload(changeEvent));
    return platformEvent;
  }

  private EntityChangeEventGeneratorRegistry createEntityChangeEventGeneratorRegistry() {
    final EntityChangeEventGeneratorRegistry registry = new EntityChangeEventGeneratorRegistry();
    registry.register(GLOBAL_TAGS_ASPECT_NAME, new GlobalTagsChangeEventGenerator());
    registry.register(GLOSSARY_TERMS_ASPECT_NAME, new GlossaryTermsChangeEventGenerator());
    registry.register(DOMAINS_ASPECT_NAME, new SingleDomainChangeEventGenerator());
    registry.register(OWNERSHIP_ASPECT_NAME, new OwnershipChangeEventGenerator());
    registry.register(STATUS_ASPECT_NAME, new StatusChangeEventGenerator());
    registry.register(DEPRECATION_ASPECT_NAME, new DeprecationChangeEventGenerator());

    // TODO Add Dataset Schema Field related change generators.

    // Entity Lifecycle change event generators
    registry.register(DATASET_KEY_ASPECT_NAME, new EntityKeyChangeEventGenerator<>());

    // Run change event generators
    registry.register(ASSERTION_RUN_EVENT_ASPECT_NAME, new AssertionRunEventChangeEventGenerator());
    registry.register(DATA_PROCESS_INSTANCE_RUN_EVENT_ASPECT_NAME,
        new DataProcessInstanceRunEventChangeEventGenerator(_mockClient, _mockAuthentication));
    return registry;
  }

  private EntityRegistry createMockEntityRegistry() {
    EntityRegistry registry = Mockito.mock(EntityRegistry.class);
    // Build Dataset Entity Spec
    EntitySpec datasetSpec = Mockito.mock(EntitySpec.class);

    AspectSpec mockTags = createMockAspectSpec(GlobalTags.class);
    Mockito.when(datasetSpec.getAspectSpec(eq(GLOBAL_TAGS_ASPECT_NAME))).thenReturn(mockTags);

    AspectSpec mockTerms = createMockAspectSpec(GlossaryTerms.class);
    Mockito.when(datasetSpec.getAspectSpec(eq(GLOSSARY_TERMS_ASPECT_NAME))).thenReturn(mockTerms);

    AspectSpec mockOwners = createMockAspectSpec(Ownership.class);
    Mockito.when(datasetSpec.getAspectSpec(eq(OWNERSHIP_ASPECT_NAME))).thenReturn(mockOwners);

    AspectSpec mockStatus = createMockAspectSpec(Status.class);
    Mockito.when(datasetSpec.getAspectSpec(eq(STATUS_ASPECT_NAME))).thenReturn(mockStatus);

    AspectSpec mockDomains = createMockAspectSpec(Domains.class);
    Mockito.when(datasetSpec.getAspectSpec(eq(DOMAINS_ASPECT_NAME))).thenReturn(mockDomains);

    AspectSpec mockDeprecation = createMockAspectSpec(Deprecation.class);
    Mockito.when(datasetSpec.getAspectSpec(eq(DEPRECATION_ASPECT_NAME))).thenReturn(mockDeprecation);

    AspectSpec mockDatasetKey = createMockAspectSpec(DatasetKey.class);
    Mockito.when(datasetSpec.getAspectSpec(eq(DATASET_KEY_ASPECT_NAME))).thenReturn(mockDatasetKey);

    Mockito.when(registry.getEntitySpec(eq(DATASET_ENTITY_NAME))).thenReturn(datasetSpec);

    // Build Assertion Entity Spec
    EntitySpec assertionSpec = Mockito.mock(EntitySpec.class);
    AspectSpec mockAssertionRunEvent = createMockAspectSpec(AssertionRunEvent.class);
    Mockito.when(assertionSpec.getAspectSpec(eq(ASSERTION_RUN_EVENT_ASPECT_NAME))).thenReturn(mockAssertionRunEvent);

    Mockito.when(registry.getEntitySpec(eq(ASSERTION_ENTITY_NAME))).thenReturn(assertionSpec);

    // Build Data Process Instance Entity Spec
    EntitySpec dataProcessInstanceSpec = Mockito.mock(EntitySpec.class);
    AspectSpec mockDataProcessInstanceRunEvent = createMockAspectSpec(DataProcessInstanceRunEvent.class);
    Mockito.when(dataProcessInstanceSpec.getAspectSpec(eq(DATA_PROCESS_INSTANCE_RUN_EVENT_ASPECT_NAME)))
        .thenReturn(mockDataProcessInstanceRunEvent);

    Mockito.when(registry.getEntitySpec(DATA_PROCESS_INSTANCE_ENTITY_NAME)).thenReturn(dataProcessInstanceSpec);

    return registry;
  }

  private void verifyProducePlatformEvent(EntityClient mockClient, PlatformEvent platformEvent) throws Exception {
    verifyProducePlatformEvent(mockClient, platformEvent, true);
  }

  private void verifyProducePlatformEvent(EntityClient mockClient, PlatformEvent platformEvent, boolean noMoreInteractions) throws Exception {
    // Verify event has been emitted.
    verify(mockClient, Mockito.times(1)).producePlatformEvent(eq(CHANGE_EVENT_PLATFORM_EVENT_NAME), Mockito.anyString(),
        argThat(new PlatformEventMatcher(platformEvent)), Mockito.any(Authentication.class));

    if (noMoreInteractions) {
      Mockito.verifyNoMoreInteractions(_mockClient);
    }
  }

  private <T extends RecordTemplate> AspectSpec createMockAspectSpec(Class<T> clazz) {
    AspectSpec mockSpec = Mockito.mock(AspectSpec.class);
    Mockito.when(mockSpec.getDataTemplateClass()).thenReturn((Class<RecordTemplate>) clazz);
    return mockSpec;
  }

  private EntityResponse buildEntityResponse(Map<String, RecordTemplate> aspects) {
    final EntityResponse entityResponse = new EntityResponse();
    final EnvelopedAspectMap aspectMap = new EnvelopedAspectMap();
    for (Map.Entry<String, RecordTemplate> entry : aspects.entrySet()) {
      aspectMap.put(entry.getKey(), new EnvelopedAspect().setValue(new Aspect(entry.getValue().data())));
    }
    entityResponse.setAspects(aspectMap);
    return entityResponse;
  }
}