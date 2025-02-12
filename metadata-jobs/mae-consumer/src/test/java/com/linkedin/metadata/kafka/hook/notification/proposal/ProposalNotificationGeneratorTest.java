package com.linkedin.metadata.kafka.hook.notification.proposal;

import static com.linkedin.metadata.Constants.CORP_GROUP_INFO_ASPECT_NAME;
import static com.linkedin.metadata.Constants.CORP_USER_INFO_ASPECT_NAME;
import static com.linkedin.metadata.Constants.DATASET_PROPERTIES_ASPECT_NAME;
import static com.linkedin.metadata.Constants.DOMAIN_PROPERTIES_ASPECT_NAME;
import static com.linkedin.metadata.Constants.GLOSSARY_TERM_INFO_ASPECT_NAME;
import static com.linkedin.metadata.Constants.NOTIFICATION_REQUEST_EVENT_NAME;
import static com.linkedin.metadata.Constants.OWNERSHIP_TYPE_INFO_ASPECT_NAME;
import static com.linkedin.metadata.Constants.STRUCTURED_PROPERTY_DEFINITION_ASPECT_NAME;
import static com.linkedin.metadata.Constants.TAG_PROPERTIES_ASPECT_NAME;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.datahub.notification.NotificationScenarioType;
import com.datahub.notification.NotificationTemplateType;
import com.datahub.notification.recipient.NotificationRecipientBuilder;
import com.datahub.notification.recipient.NotificationRecipientBuilders;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.linkedin.actionrequest.ActionRequestInfo;
import com.linkedin.actionrequest.ActionRequestParams;
import com.linkedin.actionrequest.ActionRequestStatus;
import com.linkedin.actionrequest.CreateGlossaryNodeProposal;
import com.linkedin.actionrequest.CreateGlossaryTermProposal;
import com.linkedin.actionrequest.DomainProposal;
import com.linkedin.actionrequest.GlossaryTermProposal;
import com.linkedin.actionrequest.OwnerProposal;
import com.linkedin.actionrequest.StructuredPropertyProposal;
import com.linkedin.actionrequest.TagProposal;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.Owner;
import com.linkedin.common.OwnerArray;
import com.linkedin.common.OwnershipSource;
import com.linkedin.common.OwnershipSourceType;
import com.linkedin.common.OwnershipType;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.featureflags.FeatureFlags;
import com.linkedin.dataset.DatasetProperties;
import com.linkedin.domain.DomainProperties;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.event.notification.NotificationRecipient;
import com.linkedin.event.notification.NotificationRecipientOriginType;
import com.linkedin.event.notification.NotificationRecipientType;
import com.linkedin.event.notification.NotificationRequest;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.glossary.GlossaryTermInfo;
import com.linkedin.identity.CorpGroupInfo;
import com.linkedin.identity.CorpUserInfo;
import com.linkedin.metadata.AcrylConstants;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.event.EventProducer;
import com.linkedin.metadata.graph.GraphClient;
import com.linkedin.metadata.kafka.hook.notification.NotificationRecipientsGeneratorExtraContext;
import com.linkedin.metadata.resource.SubResourceType;
import com.linkedin.metadata.service.SettingsService;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeLog;
import com.linkedin.mxe.PlatformEvent;
import com.linkedin.ownership.OwnershipTypeInfo;
import com.linkedin.structured.PrimitivePropertyValue;
import com.linkedin.structured.PrimitivePropertyValueArray;
import com.linkedin.structured.StructuredPropertyDefinition;
import com.linkedin.structured.StructuredPropertyValueAssignment;
import com.linkedin.structured.StructuredPropertyValueAssignmentArray;
import com.linkedin.subscription.EntityChangeType;
import com.linkedin.tag.TagProperties;
import io.datahubproject.metadata.context.ActorContext;
import io.datahubproject.metadata.context.OperationContext;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.*;
import org.mockito.*;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ProposalNotificationGeneratorTest {

  private static final Urn TEST_DATASET_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:snowflake,test,PROD)");
  private static final Urn TEST_TAG_URN = UrnUtils.getUrn("urn:li:tag:test");
  private static final Urn TEST_GLOSSARY_TERM_URN = UrnUtils.getUrn("urn:li:glossaryTerm:test");
  private static final Urn TEST_STRUCTURED_PROPERTY_URN =
      UrnUtils.getUrn("urn:li:structuredProperty:test");
  private static final Urn TEST_USER_CREATOR_URN = UrnUtils.getUrn("urn:li:corpuser:creator");
  private static final Urn TEST_USER_OWNER_URN = UrnUtils.getUrn("urn:li:corpuser:testUser");
  private static final Urn TEST_GROUP_OWNER_URN = UrnUtils.getUrn("urn:li:corpGroup:testGroup");
  private static final Urn TEST_DOMAIN_URN = UrnUtils.getUrn("urn:li:domain:test");
  private static final Urn TEST_OWNERSHIP_TYPE_URN = UrnUtils.getUrn("urn:li:ownershipType:test");

  @Mock private OperationContext mockSystemOpContext;

  @Mock private ActorContext mockActorContext;

  @Mock private EventProducer mockEventProducer;

  @Mock private SystemEntityClient mockEntityClient;

  @Mock private GraphClient mockGraphClient;

  @Mock private SettingsService mockSettingsService;

  @Mock private NotificationRecipientBuilders mockRecipientBuilders;

  @Mock private FeatureFlags mockFeatureFlags;

  @InjectMocks private ProposalNotificationGenerator proposalNotificationGenerator;

  @Captor private ArgumentCaptor<PlatformEvent> platformEventArgumentCaptor;

  @BeforeMethod
  public void setup() throws Exception {
    MockitoAnnotations.initMocks(this);

    // Default stubbing
    when(mockFeatureFlags.isSubscriptionsEnabled()).thenReturn(true);

    // Set up recipient builders.
    NotificationRecipientBuilder mockBuilder = mock(NotificationRecipientBuilder.class);

    NotificationRecipient testUserNotificationRecipient = new NotificationRecipient();
    testUserNotificationRecipient.setId("testuser@gmail.com");
    testUserNotificationRecipient.setOrigin(NotificationRecipientOriginType.ACTOR_NOTIFICATION);
    testUserNotificationRecipient.setActor(UrnUtils.getUrn("urn:li:corpuser:testUser"));
    testUserNotificationRecipient.setDisplayName("Test User");
    testUserNotificationRecipient.setType(NotificationRecipientType.EMAIL);

    NotificationRecipient testGlobalNotificationRecipient = new NotificationRecipient();
    testGlobalNotificationRecipient.setId("testglobal@gmail.com");
    testGlobalNotificationRecipient.setOrigin(NotificationRecipientOriginType.GLOBAL_NOTIFICATION);
    testGlobalNotificationRecipient.setType(NotificationRecipientType.EMAIL);

    when(mockBuilder.buildActorRecipients(any(), any(), any()))
        .thenReturn(Collections.singletonList(testUserNotificationRecipient));

    when(mockBuilder.buildGlobalRecipients(any(), any()))
        .thenReturn(Collections.singletonList(testGlobalNotificationRecipient));

    when(mockRecipientBuilders.listBuilders()).thenReturn(Collections.singletonList(mockBuilder));

    // Init entity client for entity name resolution.
    DatasetProperties datasetProperties = new DatasetProperties();
    datasetProperties.setQualifiedName("Test Dataset Name");
    datasetProperties.setName("Test Dataset Name");

    when(mockEntityClient.batchGetV2(
            any(OperationContext.class),
            eq(Constants.DATASET_ENTITY_NAME),
            eq(ImmutableSet.of(TEST_DATASET_URN)),
            eq(ImmutableSet.of(DATASET_PROPERTIES_ASPECT_NAME))))
        .thenReturn(
            ImmutableMap.of(
                TEST_DATASET_URN,
                new EntityResponse()
                    .setEntityName(Constants.DATASET_ENTITY_NAME)
                    .setUrn(TEST_DATASET_URN)
                    .setAspects(
                        new EnvelopedAspectMap(
                            Collections.singletonMap(
                                DATASET_PROPERTIES_ASPECT_NAME,
                                new EnvelopedAspect()
                                    .setValue(new Aspect(datasetProperties.data())))))));

    TagProperties tagProperties = new TagProperties();
    tagProperties.setName("Test Tag Name");

    when(mockEntityClient.batchGetV2(
            any(OperationContext.class),
            eq(Constants.TAG_ENTITY_NAME),
            eq(ImmutableSet.of(TEST_TAG_URN)),
            eq(ImmutableSet.of(TAG_PROPERTIES_ASPECT_NAME))))
        .thenReturn(
            ImmutableMap.of(
                TEST_TAG_URN,
                new EntityResponse()
                    .setEntityName(Constants.TAG_ENTITY_NAME)
                    .setUrn(TEST_TAG_URN)
                    .setAspects(
                        new EnvelopedAspectMap(
                            Collections.singletonMap(
                                TAG_PROPERTIES_ASPECT_NAME,
                                new EnvelopedAspect()
                                    .setValue(new Aspect(tagProperties.data())))))));

    DomainProperties domainProperties = new DomainProperties();
    domainProperties.setName("Test Domain Name");

    when(mockEntityClient.batchGetV2(
            any(OperationContext.class),
            eq(Constants.DOMAIN_ENTITY_NAME),
            eq(ImmutableSet.of(TEST_DOMAIN_URN)),
            eq(ImmutableSet.of(DOMAIN_PROPERTIES_ASPECT_NAME))))
        .thenReturn(
            ImmutableMap.of(
                TEST_DOMAIN_URN,
                new EntityResponse()
                    .setEntityName(Constants.DOMAIN_ENTITY_NAME)
                    .setUrn(TEST_DOMAIN_URN)
                    .setAspects(
                        new EnvelopedAspectMap(
                            Collections.singletonMap(
                                DOMAIN_PROPERTIES_ASPECT_NAME,
                                new EnvelopedAspect()
                                    .setValue(new Aspect(domainProperties.data())))))));

    GlossaryTermInfo glossaryTermInfo = new GlossaryTermInfo();
    glossaryTermInfo.setName("Test Term Name");

    when(mockEntityClient.batchGetV2(
            any(OperationContext.class),
            eq(Constants.GLOSSARY_TERM_ENTITY_NAME),
            eq(ImmutableSet.of(TEST_GLOSSARY_TERM_URN)),
            eq(ImmutableSet.of(GLOSSARY_TERM_INFO_ASPECT_NAME))))
        .thenReturn(
            ImmutableMap.of(
                TEST_GLOSSARY_TERM_URN,
                new EntityResponse()
                    .setEntityName(Constants.GLOSSARY_TERM_ENTITY_NAME)
                    .setUrn(TEST_GLOSSARY_TERM_URN)
                    .setAspects(
                        new EnvelopedAspectMap(
                            Collections.singletonMap(
                                GLOSSARY_TERM_INFO_ASPECT_NAME,
                                new EnvelopedAspect()
                                    .setValue(new Aspect(glossaryTermInfo.data())))))));

    StructuredPropertyDefinition structuredPropertyDefinition = new StructuredPropertyDefinition();
    structuredPropertyDefinition.setDisplayName("Test Property Name");

    when(mockEntityClient.batchGetV2(
            any(OperationContext.class),
            eq(Constants.STRUCTURED_PROPERTY_ENTITY_NAME),
            eq(ImmutableSet.of(TEST_STRUCTURED_PROPERTY_URN)),
            eq(ImmutableSet.of(STRUCTURED_PROPERTY_DEFINITION_ASPECT_NAME))))
        .thenReturn(
            ImmutableMap.of(
                TEST_STRUCTURED_PROPERTY_URN,
                new EntityResponse()
                    .setEntityName(Constants.STRUCTURED_PROPERTY_ENTITY_NAME)
                    .setUrn(TEST_STRUCTURED_PROPERTY_URN)
                    .setAspects(
                        new EnvelopedAspectMap(
                            Collections.singletonMap(
                                STRUCTURED_PROPERTY_DEFINITION_ASPECT_NAME,
                                new EnvelopedAspect()
                                    .setValue(new Aspect(structuredPropertyDefinition.data())))))));

    OwnershipTypeInfo ownershipTypeInfo = new OwnershipTypeInfo();
    ownershipTypeInfo.setName("Test Ownership Type Name");

    when(mockEntityClient.batchGetV2(
            any(OperationContext.class),
            eq(Constants.OWNERSHIP_TYPE_ENTITY_NAME),
            eq(ImmutableSet.of(TEST_OWNERSHIP_TYPE_URN)),
            eq(ImmutableSet.of(OWNERSHIP_TYPE_INFO_ASPECT_NAME))))
        .thenReturn(
            ImmutableMap.of(
                TEST_OWNERSHIP_TYPE_URN,
                new EntityResponse()
                    .setEntityName(Constants.OWNERSHIP_TYPE_ENTITY_NAME)
                    .setUrn(TEST_OWNERSHIP_TYPE_URN)
                    .setAspects(
                        new EnvelopedAspectMap(
                            Collections.singletonMap(
                                OWNERSHIP_TYPE_INFO_ASPECT_NAME,
                                new EnvelopedAspect()
                                    .setValue(new Aspect(ownershipTypeInfo.data())))))));

    CorpUserInfo ownerCorpUserInfo = new CorpUserInfo();
    ownerCorpUserInfo.setDisplayName("Test User Owner Name");

    when(mockEntityClient.batchGetV2(
            any(OperationContext.class),
            eq(Constants.CORP_USER_ENTITY_NAME),
            eq(ImmutableSet.of(TEST_USER_OWNER_URN)),
            any(Set.class)))
        .thenReturn(
            ImmutableMap.of(
                TEST_USER_OWNER_URN,
                new EntityResponse()
                    .setEntityName(Constants.CORP_USER_ENTITY_NAME)
                    .setUrn(TEST_USER_OWNER_URN)
                    .setAspects(
                        new EnvelopedAspectMap(
                            Collections.singletonMap(
                                CORP_USER_INFO_ASPECT_NAME,
                                new EnvelopedAspect()
                                    .setValue(new Aspect(ownerCorpUserInfo.data())))))));

    CorpUserInfo creatorCorpUserInfo = new CorpUserInfo();
    creatorCorpUserInfo.setDisplayName("Test Creator Name");

    when(mockEntityClient.batchGetV2(
            any(OperationContext.class),
            eq(Constants.CORP_USER_ENTITY_NAME),
            eq(ImmutableSet.of(TEST_USER_CREATOR_URN)),
            any(Set.class)))
        .thenReturn(
            ImmutableMap.of(
                TEST_USER_CREATOR_URN,
                new EntityResponse()
                    .setEntityName(Constants.CORP_USER_ENTITY_NAME)
                    .setUrn(TEST_USER_CREATOR_URN)
                    .setAspects(
                        new EnvelopedAspectMap(
                            Collections.singletonMap(
                                CORP_USER_INFO_ASPECT_NAME,
                                new EnvelopedAspect()
                                    .setValue(new Aspect(creatorCorpUserInfo.data())))))));

    CorpGroupInfo ownerCorpGroupInfo = new CorpGroupInfo();
    ownerCorpGroupInfo.setDisplayName("Test Group Owner Name");

    when(mockEntityClient.batchGetV2(
            any(OperationContext.class),
            eq(Constants.CORP_GROUP_ENTITY_NAME),
            eq(ImmutableSet.of(TEST_GROUP_OWNER_URN)),
            any(Set.class)))
        .thenReturn(
            ImmutableMap.of(
                TEST_GROUP_OWNER_URN,
                new EntityResponse()
                    .setEntityName(Constants.CORP_GROUP_ENTITY_NAME)
                    .setUrn(TEST_GROUP_OWNER_URN)
                    .setAspects(
                        new EnvelopedAspectMap(
                            Collections.singletonMap(
                                CORP_GROUP_INFO_ASPECT_NAME,
                                new EnvelopedAspect()
                                    .setValue(new Aspect(ownerCorpGroupInfo.data())))))));

    // Typical OperationContext stubbing
    when(mockSystemOpContext.getActorContext()).thenReturn(mockActorContext);
    when(mockActorContext.getActorUrn()).thenReturn(UrnUtils.getUrn("urn:li:corpuser:fake_actor"));
  }

  // -----------------------------------------------------------------------------------------------
  // 1. Test the constructor
  // -----------------------------------------------------------------------------------------------
  @Test
  public void testConstructor() {
    // Just verify that the object is constructed without error.
    assertNotNull(proposalNotificationGenerator);
  }

  // -----------------------------------------------------------------------------------------------
  // 2. Test isEligibleForSubscriberRecipients()
  // -----------------------------------------------------------------------------------------------
  @Test
  public void testIsEligibleForSubscriberRecipientsWhenSubscriptionsEnabled() {
    when(mockFeatureFlags.isSubscriptionsEnabled()).thenReturn(true);
    assertTrue(proposalNotificationGenerator.isEligibleForSubscriberRecipients());
  }

  @Test
  public void testIsEligibleForSubscriberRecipientsWhenSubscriptionsDisabled() {
    when(mockFeatureFlags.isSubscriptionsEnabled()).thenReturn(false);
    assertFalse(proposalNotificationGenerator.isEligibleForSubscriberRecipients());
  }

  // -----------------------------------------------------------------------------------------------
  // 3. Test isEligibleForCustomRecipients(NotificationScenarioType scenarioType)
  // -----------------------------------------------------------------------------------------------
  @Test
  public void testIsEligibleForCustomRecipientsForNewProposal() {
    assertTrue(
        proposalNotificationGenerator.isEligibleForCustomRecipients(
            NotificationScenarioType.NEW_PROPOSAL));
  }

  @Test
  public void testIsEligibleForCustomRecipientsForProposalStatusChange() {
    assertTrue(
        proposalNotificationGenerator.isEligibleForCustomRecipients(
            NotificationScenarioType.PROPOSAL_STATUS_CHANGE));
  }

  @Test
  public void testIsEligibleForCustomRecipientsForOtherScenario() {
    assertFalse(
        proposalNotificationGenerator.isEligibleForCustomRecipients(
            NotificationScenarioType.ENTITY_OWNER_CHANGE));
  }

  // -----------------------------------------------------------------------------------------------
  // 4. Test buildCustomRecipients(...)
  //
  //    We'll check that for NEW_PROPOSAL & PROPOSAL_STATUS_CHANGE, we get a (possibly) non-null
  // list.
  //    For everything else, we expect null.
  // -----------------------------------------------------------------------------------------------
  @Test
  public void testBuildCustomRecipientsNewProposal() {
    Urn testEntityUrn = UrnUtils.getUrn("urn:li:actionRequest:123");
    NotificationRecipientsGeneratorExtraContext extraContext =
        new NotificationRecipientsGeneratorExtraContext();

    // Provide a fake ActionRequestInfo in the extraContext
    ActionRequestInfo mockInfo = new ActionRequestInfo();
    mockInfo.setAssignedUsers(
        new UrnArray(ImmutableList.of(UrnUtils.getUrn("urn:li:corpuser:assigneeUser"))));
    mockInfo.setAssignedGroups(
        new UrnArray(ImmutableList.of(UrnUtils.getUrn("urn:li:corpGroup:assigneeGroup"))));
    mockInfo.setAssignedRoles(new UrnArray());

    extraContext.setOriginalAspect(mockInfo);

    // We'll do minimal stubbing to avoid NPE in the method
    List<NotificationRecipient> result =
        proposalNotificationGenerator.buildCustomRecipients(
            mockSystemOpContext,
            NotificationScenarioType.NEW_PROPOSAL,
            testEntityUrn,
            null,
            UrnUtils.getUrn("urn:li:corpuser:triggeringActor"),
            extraContext);

    assertNotNull(result, "For NEW_PROPOSAL, should not return null (based on code).");
    assertEquals(result.size(), 1, "We expect one recipient based on mocked recipient builders");
    assertEquals(result.get(0).getId(), "testuser@gmail.com");
  }

  @Test
  public void testBuildCustomRecipientsProposalStatusChange() {
    Urn testEntityUrn = UrnUtils.getUrn("urn:li:actionRequest:456");
    NotificationRecipientsGeneratorExtraContext extraContext =
        new NotificationRecipientsGeneratorExtraContext();
    ActionRequestInfo mockInfo = new ActionRequestInfo();
    mockInfo.setAssignedUsers(new UrnArray());
    mockInfo.setAssignedGroups(new UrnArray());
    mockInfo.setAssignedRoles(new UrnArray());
    mockInfo.setCreatedBy(UrnUtils.getUrn("urn:li:corpuser:creator"));
    extraContext.setOriginalAspect(mockInfo);

    List<NotificationRecipient> result =
        proposalNotificationGenerator.buildCustomRecipients(
            mockSystemOpContext,
            NotificationScenarioType.PROPOSAL_STATUS_CHANGE,
            testEntityUrn,
            null,
            UrnUtils.getUrn("urn:li:corpuser:someActor"),
            extraContext);

    assertNotNull(result, "For PROPOSAL_STATUS_CHANGE, should not return null (based on code).");
    assertEquals(result.size(), 1, "We expect one recipient based on mocked recipient builders");
    assertEquals(result.get(0).getId(), "testuser@gmail.com");
  }

  @Test
  public void testBuildCustomRecipientsForUnrelatedScenario() {
    Urn testEntityUrn = UrnUtils.getUrn("urn:li:actionRequest:789");
    List<NotificationRecipient> result =
        proposalNotificationGenerator.buildCustomRecipients(
            mockSystemOpContext,
            NotificationScenarioType.ENTITY_OWNER_CHANGE,
            testEntityUrn,
            EntityChangeType.OWNER_ADDED,
            UrnUtils.getUrn("urn:li:corpuser:someone"),
            null);
    assertNull(result, "For unrelated scenario, we expect null");
  }

  @Test
  public void testGenerateIgnoresWhenEntityUrnIsNull() {
    MetadataChangeLog event = new MetadataChangeLog();
    // no entityUrn
    event.setAspectName(Constants.ACTION_REQUEST_INFO_ASPECT_NAME);
    event.setChangeType(ChangeType.CREATE);

    proposalNotificationGenerator.generate(event);

    // We expect no notifications, so verify eventProducer was not called.
    verifyNoInteractions(mockEventProducer);
  }

  @Test
  public void testGenerateIgnoresWhenAspectIsNull() {
    MetadataChangeLog event = new MetadataChangeLog();
    event.setEntityUrn(UrnUtils.getUrn("urn:li:actionRequest:123"));
    // aspect is null
    proposalNotificationGenerator.generate(event);

    verifyNoInteractions(mockEventProducer);
  }

  @Test
  public void testGenerateIgnoresWhenAspectNameNotActionRequest() {
    MetadataChangeLog event = new MetadataChangeLog();
    event.setEntityUrn(UrnUtils.getUrn("urn:li:actionRequest:999"));
    event.setAspectName("SomeOtherAspect");
    event.setAspect(GenericRecordUtils.serializeAspect(new ActionRequestInfo()));
    event.setChangeType(ChangeType.CREATE);

    proposalNotificationGenerator.generate(event);
    verifyNoInteractions(mockEventProducer);
  }

  @Test
  public void testGenerateIgnoresStatusChangeWhenStatusNotComplete() {
    MetadataChangeLog event = new MetadataChangeLog();
    event.setEntityUrn(UrnUtils.getUrn("urn:li:actionRequest:456"));
    event.setAspectName(Constants.ACTION_REQUEST_STATUS_ASPECT_NAME);
    event.setChangeType(ChangeType.UPSERT);

    // Provide new status that is not COMPLETE => won't pass isProposalStatusChange
    ActionRequestStatus status = new ActionRequestStatus();
    status.setStatus("IN_PROGRESS");
    event.setAspect(GenericRecordUtils.serializeAspect(status));

    proposalNotificationGenerator.generate(event);

    // No interactions with the eventProducer
    verifyNoInteractions(mockEventProducer);
  }

  // -----------------------------------------------------------------------------------------------
  // 6. Test generateNewProposalNotifications(...) directly
  // -----------------------------------------------------------------------------------------------
  @Test
  public void testGenerateNewProposalNotificationsNoRecipients() {
    Urn actionRequestUrn = UrnUtils.getUrn("urn:li:actionRequest:111");
    ActionRequestInfo info = new ActionRequestInfo();
    // Provide a recognized type
    info.setType(AcrylConstants.ACTION_REQUEST_TYPE_TAG_PROPOSAL);

    proposalNotificationGenerator.generateNewProposalNotifications(
        actionRequestUrn,
        info,
        new AuditStamp().setActor(UrnUtils.getUrn("urn:li:corpuser:someActor")));

    // If no recipients are found (the code attempts to build them),
    // the code eventually returns. We expect no notification is sent to the event producer.
    verifyNoInteractions(mockEventProducer);
  }

  @Test
  public void testGenerateNewProposalNotificationsWithRecipients() {
    Urn actionRequestUrn = UrnUtils.getUrn("urn:li:actionRequest:112");
    ActionRequestInfo info = new ActionRequestInfo();
    info.setType(AcrylConstants.ACTION_REQUEST_TYPE_TAG_PROPOSAL);
    info.setParams(
        new ActionRequestParams()
            .setTagProposal(
                new TagProposal()
                    .setTags(new UrnArray(ImmutableList.of(UrnUtils.getUrn("urn:li:tag:tag1"))))));
    info.setResource(TEST_DATASET_URN.toString());

    Urn recipientUrn = UrnUtils.getUrn("urn:li:corpuser:recipient");

    info.setAssignedUsers(new UrnArray(Collections.singletonList(recipientUrn)));

    // We'll do partial stubbing so that buildRecipients returns some dummy recipients
    NotificationRecipient mockRecipient = new NotificationRecipient();
    mockRecipient.setActor(recipientUrn);
    mockRecipient.setId("test@test.com");
    mockRecipient.setType(NotificationRecipientType.EMAIL);
    mockRecipient.setOrigin(NotificationRecipientOriginType.ACTOR_NOTIFICATION);
    List<NotificationRecipient> recipientList = Collections.singletonList(mockRecipient);

    ProposalNotificationGenerator spyGenerator = Mockito.spy(proposalNotificationGenerator);

    // Force the buildRecipients call to return a non-empty set
    doReturn(new ArrayList<>(recipientList))
        .when(spyGenerator)
        .buildRecipients(
            any(),
            eq(NotificationScenarioType.NEW_PROPOSAL),
            eq(actionRequestUrn),
            any(),
            any(),
            any());

    Urn actorUrn = UrnUtils.getUrn("urn:li:corpuser:someActor");

    spyGenerator.generateNewProposalNotifications(
        actionRequestUrn, info, new AuditStamp().setActor(actorUrn));

    // Now we expect a NotificationRequest to be produced
    verify(mockEventProducer, atLeast(1))
        .producePlatformEvent(
            Mockito.eq(NOTIFICATION_REQUEST_EVENT_NAME),
            Mockito.eq(null),
            platformEventArgumentCaptor.capture());

    // We can examine the captured NotificationRequest, check template, recipients, etc.
    PlatformEvent platformEvent = platformEventArgumentCaptor.getValue();
    assertNotNull(platformEvent, "PlatformEvent should be produced");

    NotificationRequest notificationRequest =
        GenericRecordUtils.deserializePayload(
            platformEvent.getPayload().getValue(), NotificationRequest.class);
    assertEquals(
        notificationRequest.getMessage().getTemplate().toString(),
        NotificationTemplateType.BROADCAST_NEW_PROPOSAL.name());

    // And it should contain our single recipient
    assertEquals(notificationRequest.getRecipients().size(), 1);
    assertEquals(notificationRequest.getRecipients().get(0).getActor(), recipientUrn);
    assertEquals(notificationRequest.getRecipients().get(0).getId(), "test@test.com");
  }

  // -----------------------------------------------------------------------------------------------
  // 7. Test generateUpdatedProposalNotifications(...) directly
  // -----------------------------------------------------------------------------------------------
  @Test
  public void testGenerateUpdatedProposalNotificationsNoRecipients() {
    Urn actionRequestUrn = UrnUtils.getUrn("urn:li:actionRequest:222");
    ActionRequestInfo info = new ActionRequestInfo();
    info.setType(AcrylConstants.ACTION_REQUEST_TYPE_TAG_PROPOSAL);

    ActionRequestStatus newStatus = new ActionRequestStatus();
    newStatus.setResult(AcrylConstants.ACTION_REQUEST_RESULT_ACCEPTED);

    proposalNotificationGenerator.generateUpdatedProposalNotifications(
        actionRequestUrn,
        info,
        newStatus,
        new AuditStamp().setActor(UrnUtils.getUrn("urn:li:corpuser:someActor")));

    // If no recipients are found => no event produced
    verifyNoInteractions(mockEventProducer);
  }

  @Test
  public void testGenerateUpdatedProposalNotificationsWithRecipients() {
    Urn actionRequestUrn = UrnUtils.getUrn("urn:li:actionRequest:223");
    ActionRequestInfo info = new ActionRequestInfo();
    info.setType(AcrylConstants.ACTION_REQUEST_TYPE_TAG_PROPOSAL);
    info.setParams(
        new ActionRequestParams()
            .setTagProposal(
                new TagProposal()
                    .setTags(
                        new UrnArray(ImmutableList.of(UrnUtils.getUrn(TEST_TAG_URN.toString()))))));
    info.setResource(TEST_DATASET_URN.toString());

    ActionRequestStatus newStatus = new ActionRequestStatus();
    newStatus.setResult(AcrylConstants.ACTION_REQUEST_RESULT_ACCEPTED);

    Urn recipientUrn = UrnUtils.getUrn("urn:li:corpuser:recipient");

    // We'll do partial mocking again to supply some recipients
    NotificationRecipient mockRecipient = new NotificationRecipient();
    mockRecipient.setActor(recipientUrn);
    mockRecipient.setId("test@test.com");
    mockRecipient.setType(NotificationRecipientType.EMAIL);
    mockRecipient.setOrigin(NotificationRecipientOriginType.ACTOR_NOTIFICATION);
    List<NotificationRecipient> recipientList = Collections.singletonList(mockRecipient);

    ProposalNotificationGenerator spyGenerator = Mockito.spy(proposalNotificationGenerator);

    doReturn(new ArrayList<>(recipientList))
        .when(spyGenerator)
        .buildRecipients(
            any(),
            eq(NotificationScenarioType.PROPOSAL_STATUS_CHANGE),
            eq(actionRequestUrn),
            any(),
            any(),
            any());

    Urn actorUrn = TEST_USER_CREATOR_URN;

    spyGenerator.generateUpdatedProposalNotifications(
        actionRequestUrn, info, newStatus, new AuditStamp().setActor(actorUrn));

    verify(mockEventProducer, atLeast(1))
        .producePlatformEvent(
            Mockito.eq(NOTIFICATION_REQUEST_EVENT_NAME),
            Mockito.eq(null),
            platformEventArgumentCaptor.capture());

    // We can examine the captured NotificationRequest, check template, recipients, etc.
    PlatformEvent platformEvent = platformEventArgumentCaptor.getValue();
    assertNotNull(platformEvent, "PlatformEvent should be produced");

    NotificationRequest notificationRequest =
        GenericRecordUtils.deserializePayload(
            platformEvent.getPayload().getValue(), NotificationRequest.class);
    assertEquals(
        notificationRequest.getMessage().getTemplate().toString(),
        NotificationTemplateType.BROADCAST_PROPOSAL_STATUS_CHANGE.name());

    assertEquals(notificationRequest.getRecipients().size(), 1);
    assertEquals(notificationRequest.getRecipients().get(0).getActor(), recipientUrn);
    assertEquals(notificationRequest.getRecipients().get(0).getId(), "test@test.com");
  }

  // -----------------------------------------------------------------------------------------------
  // Verification of all of the parameter types.
  // -----------------------------------------------------------------------------------------------
  @Test
  public void testTemplateParamsArePopulatedForNewAssetTagProposal() {
    Urn actionRequestUrn = UrnUtils.getUrn("urn:li:actionRequest:444");
    ActionRequestInfo info = new ActionRequestInfo();
    info.setType(AcrylConstants.ACTION_REQUEST_TYPE_TAG_PROPOSAL);
    info.setParams(
        new ActionRequestParams()
            .setTagProposal(
                new TagProposal()
                    .setTags(
                        new UrnArray(
                            Collections.singletonList(UrnUtils.getUrn(TEST_TAG_URN.toString()))))));
    info.setResource(TEST_DATASET_URN.toString());

    // We'll ensure that at least one recipient is generated
    ProposalNotificationGenerator spyGenerator = Mockito.spy(proposalNotificationGenerator);

    Urn recipientUrn = UrnUtils.getUrn("urn:li:corpuser:recipient");

    NotificationRecipient mockRecipient = new NotificationRecipient();
    mockRecipient.setActor(recipientUrn);
    mockRecipient.setId("test@test.com");
    mockRecipient.setType(NotificationRecipientType.EMAIL);
    mockRecipient.setOrigin(NotificationRecipientOriginType.ACTOR_NOTIFICATION);

    doReturn(Collections.singletonList(mockRecipient))
        .when(spyGenerator)
        .buildRecipients(
            any(),
            eq(NotificationScenarioType.NEW_PROPOSAL),
            eq(actionRequestUrn),
            any(),
            any(),
            any());

    // Trigger
    spyGenerator.generateNewProposalNotifications(
        actionRequestUrn, info, new AuditStamp().setActor(TEST_USER_CREATOR_URN));

    // Capture
    verify(mockEventProducer, atLeast(1))
        .producePlatformEvent(
            Mockito.eq(NOTIFICATION_REQUEST_EVENT_NAME),
            Mockito.eq(null),
            platformEventArgumentCaptor.capture());

    // We can examine the captured NotificationRequest, check template, recipients, etc.
    PlatformEvent platformEvent = platformEventArgumentCaptor.getValue();
    assertNotNull(platformEvent, "PlatformEvent should be produced");

    NotificationRequest notificationRequest =
        GenericRecordUtils.deserializePayload(
            platformEvent.getPayload().getValue(), NotificationRequest.class);
    assertEquals(
        notificationRequest.getMessage().getTemplate().toString(),
        NotificationTemplateType.BROADCAST_NEW_PROPOSAL.name());

    // Check the templateParams
    Map<String, String> templateParams = notificationRequest.getMessage().getParameters();
    // For the scenario type TAG_PROPOSAL -> operation=add, etc.
    assertEquals(templateParams.get("operation"), "add");
    assertEquals(templateParams.get("modifierType"), "Tag(s)");
    assertEquals(templateParams.get("modifierNames"), "[\"Test Tag Name\"]");
    assertEquals(
        templateParams.get("modifierPaths"),
        String.format("[\"/tag/%s\"]", urlEncode(TEST_TAG_URN)));
    assertEquals(templateParams.get("entityName"), "Test Dataset Name");
    assertEquals(templateParams.get("entityType"), "Dataset");
    assertEquals(
        templateParams.get("entityPath"),
        String.format("/dataset/%s", urlEncode(TEST_DATASET_URN)));
    assertEquals(templateParams.get("actorUrn"), TEST_USER_CREATOR_URN.toString());
    assertEquals(templateParams.get("actorName"), "Test Creator Name");
  }

  @Test
  public void testTemplateParamsArePopulatedForAssetTagProposalStatusChange() {
    Urn actionRequestUrn = UrnUtils.getUrn("urn:li:actionRequest:555");
    ActionRequestInfo info = new ActionRequestInfo();
    info.setType(AcrylConstants.ACTION_REQUEST_TYPE_TAG_PROPOSAL);
    info.setParams(
        new ActionRequestParams()
            .setTagProposal(
                new TagProposal()
                    .setTags(
                        new UrnArray(
                            Collections.singletonList(UrnUtils.getUrn(TEST_TAG_URN.toString()))))));
    info.setResource(TEST_DATASET_URN.toString());

    ActionRequestStatus newStatus = new ActionRequestStatus();
    newStatus.setResult(AcrylConstants.ACTION_REQUEST_RESULT_ACCEPTED);

    // We'll ensure that at least one recipient is generated
    ProposalNotificationGenerator spyGenerator = Mockito.spy(proposalNotificationGenerator);

    Urn recipientUrn = UrnUtils.getUrn("urn:li:corpuser:recipient");

    NotificationRecipient mockRecipient = new NotificationRecipient();
    mockRecipient.setActor(recipientUrn);
    mockRecipient.setId("test@test.com");
    mockRecipient.setType(NotificationRecipientType.EMAIL);
    mockRecipient.setOrigin(NotificationRecipientOriginType.ACTOR_NOTIFICATION);

    doReturn(Collections.singletonList(mockRecipient))
        .when(spyGenerator)
        .buildRecipients(
            any(),
            eq(NotificationScenarioType.PROPOSAL_STATUS_CHANGE),
            eq(actionRequestUrn),
            any(),
            any(),
            any());

    spyGenerator.generateUpdatedProposalNotifications(
        actionRequestUrn, info, newStatus, new AuditStamp().setActor(TEST_USER_CREATOR_URN));

    // Capture
    verify(mockEventProducer, atLeast(1))
        .producePlatformEvent(
            Mockito.eq(NOTIFICATION_REQUEST_EVENT_NAME),
            Mockito.eq(null),
            platformEventArgumentCaptor.capture());

    PlatformEvent platformEvent = platformEventArgumentCaptor.getValue();
    assertNotNull(platformEvent, "PlatformEvent should be produced");

    NotificationRequest notificationRequest =
        GenericRecordUtils.deserializePayload(
            platformEvent.getPayload().getValue(), NotificationRequest.class);

    // Check the templateParams
    Map<String, String> templateParams = notificationRequest.getMessage().getParameters();

    // We expect "action" => "accepted"
    assertEquals(templateParams.get("operation"), "add");
    assertEquals(templateParams.get("modifierType"), "Tag(s)");
    assertEquals(templateParams.get("modifierNames"), "[\"Test Tag Name\"]");
    assertEquals(
        templateParams.get("modifierPaths"),
        String.format("[\"/tag/%s\"]", urlEncode(TEST_TAG_URN)));
    assertEquals(templateParams.get("entityName"), "Test Dataset Name");
    assertEquals(templateParams.get("entityType"), "Dataset");
    assertEquals(
        templateParams.get("entityPath"),
        String.format("/dataset/%s", urlEncode(TEST_DATASET_URN)));
    assertEquals(templateParams.get("actorUrn"), TEST_USER_CREATOR_URN.toString());
    assertEquals(templateParams.get("actorName"), "Test Creator Name");
    assertEquals(templateParams.get("action"), "accepted");
  }

  @Test
  public void testTemplateParamsArePopulatedForNewSchemaFieldTagProposal() {
    Urn actionRequestUrn = UrnUtils.getUrn("urn:li:actionRequest:444");
    ActionRequestInfo info = new ActionRequestInfo();
    info.setType(AcrylConstants.ACTION_REQUEST_TYPE_TAG_PROPOSAL);
    info.setParams(
        new ActionRequestParams()
            .setTagProposal(
                new TagProposal()
                    .setTags(
                        new UrnArray(
                            Collections.singletonList(UrnUtils.getUrn(TEST_TAG_URN.toString()))))));
    info.setResource(TEST_DATASET_URN.toString());
    info.setSubResourceType(SubResourceType.DATASET_FIELD.toString());
    info.setSubResource("testField");

    // We'll ensure that at least one recipient is generated
    ProposalNotificationGenerator spyGenerator = Mockito.spy(proposalNotificationGenerator);

    Urn recipientUrn = UrnUtils.getUrn("urn:li:corpuser:recipient");

    NotificationRecipient mockRecipient = new NotificationRecipient();
    mockRecipient.setActor(recipientUrn);
    mockRecipient.setId("test@test.com");
    mockRecipient.setType(NotificationRecipientType.EMAIL);
    mockRecipient.setOrigin(NotificationRecipientOriginType.ACTOR_NOTIFICATION);

    doReturn(Collections.singletonList(mockRecipient))
        .when(spyGenerator)
        .buildRecipients(
            any(),
            eq(NotificationScenarioType.NEW_PROPOSAL),
            eq(actionRequestUrn),
            any(),
            any(),
            any());

    // Trigger
    spyGenerator.generateNewProposalNotifications(
        actionRequestUrn, info, new AuditStamp().setActor(TEST_USER_CREATOR_URN));

    // Capture
    verify(mockEventProducer, atLeast(1))
        .producePlatformEvent(
            Mockito.eq(NOTIFICATION_REQUEST_EVENT_NAME),
            Mockito.eq(null),
            platformEventArgumentCaptor.capture());

    // We can examine the captured NotificationRequest, check template, recipients, etc.
    PlatformEvent platformEvent = platformEventArgumentCaptor.getValue();
    assertNotNull(platformEvent, "PlatformEvent should be produced");

    NotificationRequest notificationRequest =
        GenericRecordUtils.deserializePayload(
            platformEvent.getPayload().getValue(), NotificationRequest.class);
    assertEquals(
        notificationRequest.getMessage().getTemplate().toString(),
        NotificationTemplateType.BROADCAST_NEW_PROPOSAL.name());

    // Check the templateParams
    Map<String, String> templateParams = notificationRequest.getMessage().getParameters();
    // For the scenario type TAG_PROPOSAL -> operation=add, etc.
    assertEquals(templateParams.get("operation"), "add");
    assertEquals(templateParams.get("modifierType"), "Tag(s)");
    assertEquals(templateParams.get("modifierNames"), "[\"Test Tag Name\"]");
    assertEquals(
        templateParams.get("modifierPaths"),
        String.format("[\"/tag/%s\"]", urlEncode(TEST_TAG_URN)));
    assertEquals(templateParams.get("entityName"), "Test Dataset Name");
    assertEquals(templateParams.get("entityType"), "Dataset");
    assertEquals(
        templateParams.get("entityPath"),
        String.format("/dataset/%s", urlEncode(TEST_DATASET_URN)));
    assertEquals(templateParams.get("actorUrn"), TEST_USER_CREATOR_URN.toString());
    assertEquals(templateParams.get("actorName"), "Test Creator Name");
    assertEquals(templateParams.get("subResourceType"), "DATASET_FIELD");
    assertEquals(templateParams.get("subResource"), "testField");
  }

  @Test
  public void testTemplateParamsArePopulatedForSchemaFieldTagProposalStatusChange() {
    Urn actionRequestUrn = UrnUtils.getUrn("urn:li:actionRequest:555");
    ActionRequestInfo info = new ActionRequestInfo();
    info.setType(AcrylConstants.ACTION_REQUEST_TYPE_TAG_PROPOSAL);
    info.setParams(
        new ActionRequestParams()
            .setTagProposal(
                new TagProposal()
                    .setTags(
                        new UrnArray(
                            Collections.singletonList(UrnUtils.getUrn(TEST_TAG_URN.toString()))))));
    info.setResource(TEST_DATASET_URN.toString());
    info.setSubResourceType(SubResourceType.DATASET_FIELD.toString());
    info.setSubResource("testField");

    ActionRequestStatus newStatus = new ActionRequestStatus();
    newStatus.setResult(AcrylConstants.ACTION_REQUEST_RESULT_ACCEPTED);

    // We'll ensure that at least one recipient is generated
    ProposalNotificationGenerator spyGenerator = Mockito.spy(proposalNotificationGenerator);

    Urn recipientUrn = UrnUtils.getUrn("urn:li:corpuser:recipient");

    NotificationRecipient mockRecipient = new NotificationRecipient();
    mockRecipient.setActor(recipientUrn);
    mockRecipient.setId("test@test.com");
    mockRecipient.setType(NotificationRecipientType.EMAIL);
    mockRecipient.setOrigin(NotificationRecipientOriginType.ACTOR_NOTIFICATION);

    doReturn(Collections.singletonList(mockRecipient))
        .when(spyGenerator)
        .buildRecipients(
            any(),
            eq(NotificationScenarioType.PROPOSAL_STATUS_CHANGE),
            eq(actionRequestUrn),
            any(),
            any(),
            any());

    spyGenerator.generateUpdatedProposalNotifications(
        actionRequestUrn, info, newStatus, new AuditStamp().setActor(TEST_USER_CREATOR_URN));

    // Capture
    verify(mockEventProducer, atLeast(1))
        .producePlatformEvent(
            Mockito.eq(NOTIFICATION_REQUEST_EVENT_NAME),
            Mockito.eq(null),
            platformEventArgumentCaptor.capture());

    PlatformEvent platformEvent = platformEventArgumentCaptor.getValue();
    assertNotNull(platformEvent, "PlatformEvent should be produced");

    NotificationRequest notificationRequest =
        GenericRecordUtils.deserializePayload(
            platformEvent.getPayload().getValue(), NotificationRequest.class);

    // Check the templateParams
    Map<String, String> templateParams = notificationRequest.getMessage().getParameters();

    // We expect "action" => "accepted"
    assertEquals(templateParams.get("operation"), "add");
    assertEquals(templateParams.get("modifierType"), "Tag(s)");
    assertEquals(templateParams.get("modifierNames"), "[\"Test Tag Name\"]");
    assertEquals(
        templateParams.get("modifierPaths"),
        String.format("[\"/tag/%s\"]", urlEncode(TEST_TAG_URN)));
    assertEquals(templateParams.get("entityName"), "Test Dataset Name");
    assertEquals(templateParams.get("entityType"), "Dataset");
    assertEquals(
        templateParams.get("entityPath"),
        String.format("/dataset/%s", urlEncode(TEST_DATASET_URN)));
    assertEquals(templateParams.get("actorUrn"), TEST_USER_CREATOR_URN.toString());
    assertEquals(templateParams.get("actorName"), "Test Creator Name");
    assertEquals(templateParams.get("action"), "accepted");
    assertEquals(templateParams.get("subResourceType"), "DATASET_FIELD");
    assertEquals(templateParams.get("subResource"), "testField");
  }

  @Test
  public void testTemplateParamsArePopulatedForNewAssetGlossaryTermProposal() {
    Urn actionRequestUrn = UrnUtils.getUrn("urn:li:actionRequest:444");
    ActionRequestInfo info = new ActionRequestInfo();
    info.setType(AcrylConstants.ACTION_REQUEST_TYPE_TERM_PROPOSAL);
    info.setParams(
        new ActionRequestParams()
            .setGlossaryTermProposal(
                new GlossaryTermProposal()
                    .setGlossaryTerms(
                        new UrnArray(
                            Collections.singletonList(
                                UrnUtils.getUrn(TEST_GLOSSARY_TERM_URN.toString()))))));
    info.setResource(TEST_DATASET_URN.toString());

    // We'll ensure that at least one recipient is generated
    ProposalNotificationGenerator spyGenerator = Mockito.spy(proposalNotificationGenerator);

    Urn recipientUrn = UrnUtils.getUrn("urn:li:corpuser:recipient");

    NotificationRecipient mockRecipient = new NotificationRecipient();
    mockRecipient.setActor(recipientUrn);
    mockRecipient.setId("test@test.com");
    mockRecipient.setType(NotificationRecipientType.EMAIL);
    mockRecipient.setOrigin(NotificationRecipientOriginType.ACTOR_NOTIFICATION);

    doReturn(Collections.singletonList(mockRecipient))
        .when(spyGenerator)
        .buildRecipients(
            any(),
            eq(NotificationScenarioType.NEW_PROPOSAL),
            eq(actionRequestUrn),
            any(),
            any(),
            any());

    // Trigger
    spyGenerator.generateNewProposalNotifications(
        actionRequestUrn, info, new AuditStamp().setActor(TEST_USER_CREATOR_URN));

    // Capture
    verify(mockEventProducer, atLeast(1))
        .producePlatformEvent(
            Mockito.eq(NOTIFICATION_REQUEST_EVENT_NAME),
            Mockito.eq(null),
            platformEventArgumentCaptor.capture());

    // We can examine the captured NotificationRequest, check template, recipients, etc.
    PlatformEvent platformEvent = platformEventArgumentCaptor.getValue();
    assertNotNull(platformEvent, "PlatformEvent should be produced");

    NotificationRequest notificationRequest =
        GenericRecordUtils.deserializePayload(
            platformEvent.getPayload().getValue(), NotificationRequest.class);
    assertEquals(
        notificationRequest.getMessage().getTemplate().toString(),
        NotificationTemplateType.BROADCAST_NEW_PROPOSAL.name());

    // Check the templateParams
    Map<String, String> templateParams = notificationRequest.getMessage().getParameters();
    // For the scenario type TAG_PROPOSAL -> operation=add, etc.
    assertEquals(templateParams.get("operation"), "add");
    assertEquals(templateParams.get("modifierType"), "Glossary Term(s)");
    assertEquals(templateParams.get("modifierNames"), "[\"Test Term Name\"]");
    assertEquals(
        templateParams.get("modifierPaths"),
        String.format("[\"/glossaryTerm/%s\"]", urlEncode(TEST_GLOSSARY_TERM_URN)));
    assertEquals(templateParams.get("entityName"), "Test Dataset Name");
    assertEquals(templateParams.get("entityType"), "Dataset");
    assertEquals(
        templateParams.get("entityPath"),
        String.format("/dataset/%s", urlEncode(TEST_DATASET_URN)));
    assertEquals(templateParams.get("actorUrn"), TEST_USER_CREATOR_URN.toString());
    assertEquals(templateParams.get("actorName"), "Test Creator Name");
  }

  @Test
  public void testTemplateParamsArePopulatedForAssetGlossaryTermProposalStatusChange() {
    Urn actionRequestUrn = UrnUtils.getUrn("urn:li:actionRequest:555");
    ActionRequestInfo info = new ActionRequestInfo();
    info.setType(AcrylConstants.ACTION_REQUEST_TYPE_TERM_PROPOSAL);
    info.setParams(
        new ActionRequestParams()
            .setGlossaryTermProposal(
                new GlossaryTermProposal()
                    .setGlossaryTerms(
                        new UrnArray(
                            Collections.singletonList(
                                UrnUtils.getUrn(TEST_GLOSSARY_TERM_URN.toString()))))));
    info.setResource(TEST_DATASET_URN.toString());

    ActionRequestStatus newStatus = new ActionRequestStatus();
    newStatus.setResult(AcrylConstants.ACTION_REQUEST_RESULT_ACCEPTED);

    // We'll ensure that at least one recipient is generated
    ProposalNotificationGenerator spyGenerator = Mockito.spy(proposalNotificationGenerator);

    Urn recipientUrn = UrnUtils.getUrn("urn:li:corpuser:recipient");

    NotificationRecipient mockRecipient = new NotificationRecipient();
    mockRecipient.setActor(recipientUrn);
    mockRecipient.setId("test@test.com");
    mockRecipient.setType(NotificationRecipientType.EMAIL);
    mockRecipient.setOrigin(NotificationRecipientOriginType.ACTOR_NOTIFICATION);

    doReturn(Collections.singletonList(mockRecipient))
        .when(spyGenerator)
        .buildRecipients(
            any(),
            eq(NotificationScenarioType.PROPOSAL_STATUS_CHANGE),
            eq(actionRequestUrn),
            any(),
            any(),
            any());

    spyGenerator.generateUpdatedProposalNotifications(
        actionRequestUrn, info, newStatus, new AuditStamp().setActor(TEST_USER_CREATOR_URN));

    // Capture
    verify(mockEventProducer, atLeast(1))
        .producePlatformEvent(
            Mockito.eq(NOTIFICATION_REQUEST_EVENT_NAME),
            Mockito.eq(null),
            platformEventArgumentCaptor.capture());

    PlatformEvent platformEvent = platformEventArgumentCaptor.getValue();
    assertNotNull(platformEvent, "PlatformEvent should be produced");

    NotificationRequest notificationRequest =
        GenericRecordUtils.deserializePayload(
            platformEvent.getPayload().getValue(), NotificationRequest.class);

    // Check the templateParams
    Map<String, String> templateParams = notificationRequest.getMessage().getParameters();

    // We expect "action" => "accepted"
    assertEquals(templateParams.get("operation"), "add");
    assertEquals(templateParams.get("modifierType"), "Glossary Term(s)");
    assertEquals(templateParams.get("modifierNames"), "[\"Test Term Name\"]");
    assertEquals(
        templateParams.get("modifierPaths"),
        String.format("[\"/glossaryTerm/%s\"]", urlEncode(TEST_GLOSSARY_TERM_URN)));
    assertEquals(templateParams.get("entityName"), "Test Dataset Name");
    assertEquals(templateParams.get("entityType"), "Dataset");
    assertEquals(
        templateParams.get("entityPath"),
        String.format("/dataset/%s", urlEncode(TEST_DATASET_URN)));
    assertEquals(templateParams.get("actorUrn"), TEST_USER_CREATOR_URN.toString());
    assertEquals(templateParams.get("actorName"), "Test Creator Name");
    assertEquals(templateParams.get("action"), "accepted");
  }

  @Test
  public void testTemplateParamsArePopulatedForNewSchemaFieldGlossaryTermProposal() {
    Urn actionRequestUrn = UrnUtils.getUrn("urn:li:actionRequest:444");
    ActionRequestInfo info = new ActionRequestInfo();
    info.setType(AcrylConstants.ACTION_REQUEST_TYPE_TERM_PROPOSAL);
    info.setParams(
        new ActionRequestParams()
            .setGlossaryTermProposal(
                new GlossaryTermProposal()
                    .setGlossaryTerms(
                        new UrnArray(
                            Collections.singletonList(
                                UrnUtils.getUrn(TEST_GLOSSARY_TERM_URN.toString()))))));
    info.setResource(TEST_DATASET_URN.toString());
    info.setSubResourceType(SubResourceType.DATASET_FIELD.toString());
    info.setSubResource("testField");

    // We'll ensure that at least one recipient is generated
    ProposalNotificationGenerator spyGenerator = Mockito.spy(proposalNotificationGenerator);

    Urn recipientUrn = UrnUtils.getUrn("urn:li:corpuser:recipient");

    NotificationRecipient mockRecipient = new NotificationRecipient();
    mockRecipient.setActor(recipientUrn);
    mockRecipient.setId("test@test.com");
    mockRecipient.setType(NotificationRecipientType.EMAIL);
    mockRecipient.setOrigin(NotificationRecipientOriginType.ACTOR_NOTIFICATION);

    doReturn(Collections.singletonList(mockRecipient))
        .when(spyGenerator)
        .buildRecipients(
            any(),
            eq(NotificationScenarioType.NEW_PROPOSAL),
            eq(actionRequestUrn),
            any(),
            any(),
            any());

    // Trigger
    spyGenerator.generateNewProposalNotifications(
        actionRequestUrn, info, new AuditStamp().setActor(TEST_USER_CREATOR_URN));

    // Capture
    verify(mockEventProducer, atLeast(1))
        .producePlatformEvent(
            Mockito.eq(NOTIFICATION_REQUEST_EVENT_NAME),
            Mockito.eq(null),
            platformEventArgumentCaptor.capture());

    // We can examine the captured NotificationRequest, check template, recipients, etc.
    PlatformEvent platformEvent = platformEventArgumentCaptor.getValue();
    assertNotNull(platformEvent, "PlatformEvent should be produced");

    NotificationRequest notificationRequest =
        GenericRecordUtils.deserializePayload(
            platformEvent.getPayload().getValue(), NotificationRequest.class);
    assertEquals(
        notificationRequest.getMessage().getTemplate().toString(),
        NotificationTemplateType.BROADCAST_NEW_PROPOSAL.name());

    // Check the templateParams
    Map<String, String> templateParams = notificationRequest.getMessage().getParameters();
    // For the scenario type TAG_PROPOSAL -> operation=add, etc.
    assertEquals(templateParams.get("operation"), "add");
    assertEquals(templateParams.get("modifierType"), "Glossary Term(s)");
    assertEquals(templateParams.get("modifierNames"), "[\"Test Term Name\"]");
    assertEquals(
        templateParams.get("modifierPaths"),
        String.format("[\"/glossaryTerm/%s\"]", urlEncode(TEST_GLOSSARY_TERM_URN)));
    assertEquals(templateParams.get("entityName"), "Test Dataset Name");
    assertEquals(templateParams.get("entityType"), "Dataset");
    assertEquals(
        templateParams.get("entityPath"),
        String.format("/dataset/%s", urlEncode(TEST_DATASET_URN)));
    assertEquals(templateParams.get("actorUrn"), TEST_USER_CREATOR_URN.toString());
    assertEquals(templateParams.get("actorName"), "Test Creator Name");
    assertEquals(templateParams.get("subResourceType"), "DATASET_FIELD");
    assertEquals(templateParams.get("subResource"), "testField");
  }

  @Test
  public void testTemplateParamsArePopulatedForSchemaFieldGlossaryTermProposalStatusChange() {
    Urn actionRequestUrn = UrnUtils.getUrn("urn:li:actionRequest:555");
    ActionRequestInfo info = new ActionRequestInfo();
    info.setType(AcrylConstants.ACTION_REQUEST_TYPE_TERM_PROPOSAL);
    info.setParams(
        new ActionRequestParams()
            .setGlossaryTermProposal(
                new GlossaryTermProposal()
                    .setGlossaryTerms(
                        new UrnArray(
                            Collections.singletonList(
                                UrnUtils.getUrn(TEST_GLOSSARY_TERM_URN.toString()))))));
    info.setResource(TEST_DATASET_URN.toString());
    info.setSubResourceType(SubResourceType.DATASET_FIELD.toString());
    info.setSubResource("testField");

    ActionRequestStatus newStatus = new ActionRequestStatus();
    newStatus.setResult(AcrylConstants.ACTION_REQUEST_RESULT_ACCEPTED);

    // We'll ensure that at least one recipient is generated
    ProposalNotificationGenerator spyGenerator = Mockito.spy(proposalNotificationGenerator);

    Urn recipientUrn = UrnUtils.getUrn("urn:li:corpuser:recipient");

    NotificationRecipient mockRecipient = new NotificationRecipient();
    mockRecipient.setActor(recipientUrn);
    mockRecipient.setId("test@test.com");
    mockRecipient.setType(NotificationRecipientType.EMAIL);
    mockRecipient.setOrigin(NotificationRecipientOriginType.ACTOR_NOTIFICATION);

    doReturn(Collections.singletonList(mockRecipient))
        .when(spyGenerator)
        .buildRecipients(
            any(),
            eq(NotificationScenarioType.PROPOSAL_STATUS_CHANGE),
            eq(actionRequestUrn),
            any(),
            any(),
            any());

    spyGenerator.generateUpdatedProposalNotifications(
        actionRequestUrn, info, newStatus, new AuditStamp().setActor(TEST_USER_CREATOR_URN));

    // Capture
    verify(mockEventProducer, atLeast(1))
        .producePlatformEvent(
            Mockito.eq(NOTIFICATION_REQUEST_EVENT_NAME),
            Mockito.eq(null),
            platformEventArgumentCaptor.capture());

    PlatformEvent platformEvent = platformEventArgumentCaptor.getValue();
    assertNotNull(platformEvent, "PlatformEvent should be produced");

    NotificationRequest notificationRequest =
        GenericRecordUtils.deserializePayload(
            platformEvent.getPayload().getValue(), NotificationRequest.class);

    // Check the templateParams
    Map<String, String> templateParams = notificationRequest.getMessage().getParameters();

    // We expect "action" => "accepted"
    assertEquals(templateParams.get("operation"), "add");
    assertEquals(templateParams.get("modifierType"), "Glossary Term(s)");
    assertEquals(templateParams.get("modifierNames"), "[\"Test Term Name\"]");
    assertEquals(
        templateParams.get("modifierPaths"),
        String.format("[\"/glossaryTerm/%s\"]", urlEncode(TEST_GLOSSARY_TERM_URN)));
    assertEquals(templateParams.get("entityName"), "Test Dataset Name");
    assertEquals(templateParams.get("entityType"), "Dataset");
    assertEquals(
        templateParams.get("entityPath"),
        String.format("/dataset/%s", urlEncode(TEST_DATASET_URN)));
    assertEquals(templateParams.get("actorUrn"), TEST_USER_CREATOR_URN.toString());
    assertEquals(templateParams.get("actorName"), "Test Creator Name");
    assertEquals(templateParams.get("action"), "accepted");
    assertEquals(templateParams.get("subResourceType"), "DATASET_FIELD");
    assertEquals(templateParams.get("subResource"), "testField");
  }

  @Test
  public void testTemplateParamsArePopulatedForCreateGlossaryTermProposal() {
    Urn actionRequestUrn = UrnUtils.getUrn("urn:li:actionRequest:444");
    ActionRequestInfo info = new ActionRequestInfo();
    info.setType(AcrylConstants.ACTION_REQUEST_TYPE_CREATE_GLOSSARY_TERM_PROPOSAL);
    Urn parentNodeUrn = UrnUtils.getUrn("urn:li:glossaryNode:123");
    info.setParams(
        new ActionRequestParams()
            .setCreateGlossaryTermProposal(
                new CreateGlossaryTermProposal()
                    .setName("test term name")
                    .setDescription("test term description")
                    .setParentNode(parentNodeUrn)));

    // We'll ensure that at least one recipient is generated
    ProposalNotificationGenerator spyGenerator = Mockito.spy(proposalNotificationGenerator);

    Urn recipientUrn = UrnUtils.getUrn("urn:li:corpuser:recipient");

    NotificationRecipient mockRecipient = new NotificationRecipient();
    mockRecipient.setActor(recipientUrn);
    mockRecipient.setId("test@test.com");
    mockRecipient.setType(NotificationRecipientType.EMAIL);
    mockRecipient.setOrigin(NotificationRecipientOriginType.ACTOR_NOTIFICATION);

    doReturn(Collections.singletonList(mockRecipient))
        .when(spyGenerator)
        .buildRecipients(
            any(),
            eq(NotificationScenarioType.NEW_PROPOSAL),
            eq(actionRequestUrn),
            any(),
            any(),
            any());

    // Trigger
    spyGenerator.generateNewProposalNotifications(
        actionRequestUrn, info, new AuditStamp().setActor(TEST_USER_CREATOR_URN));

    // Capture
    verify(mockEventProducer, atLeast(1))
        .producePlatformEvent(
            Mockito.eq(NOTIFICATION_REQUEST_EVENT_NAME),
            Mockito.eq(null),
            platformEventArgumentCaptor.capture());

    // We can examine the captured NotificationRequest, check template, recipients, etc.
    PlatformEvent platformEvent = platformEventArgumentCaptor.getValue();
    assertNotNull(platformEvent, "PlatformEvent should be produced");

    NotificationRequest notificationRequest =
        GenericRecordUtils.deserializePayload(
            platformEvent.getPayload().getValue(), NotificationRequest.class);
    assertEquals(
        notificationRequest.getMessage().getTemplate().toString(),
        NotificationTemplateType.BROADCAST_NEW_PROPOSAL.name());

    // Check the templateParams
    Map<String, String> templateParams = notificationRequest.getMessage().getParameters();
    // For the scenario type TAG_PROPOSAL -> operation=add, etc.
    assertEquals(templateParams.get("operation"), "create");
    assertEquals(templateParams.get("entityName"), "test term name");
    assertEquals(templateParams.get("entityType"), "Glossary Term");
    assertEquals(templateParams.get("actorUrn"), TEST_USER_CREATOR_URN.toString());
    assertEquals(templateParams.get("actorName"), "Test Creator Name");
  }

  @Test
  public void testTemplateParamsArePopulatedForCreateGlossaryTermProposalStatusChange() {
    Urn actionRequestUrn = UrnUtils.getUrn("urn:li:actionRequest:555");
    ActionRequestInfo info = new ActionRequestInfo();
    info.setType(AcrylConstants.ACTION_REQUEST_TYPE_CREATE_GLOSSARY_TERM_PROPOSAL);
    Urn parentNodeUrn = UrnUtils.getUrn("urn:li:glossaryNode:123");
    info.setParams(
        new ActionRequestParams()
            .setCreateGlossaryTermProposal(
                new CreateGlossaryTermProposal()
                    .setName("test term name")
                    .setDescription("test term description")
                    .setParentNode(parentNodeUrn)));

    ActionRequestStatus newStatus = new ActionRequestStatus();
    newStatus.setResult(AcrylConstants.ACTION_REQUEST_RESULT_ACCEPTED);

    // We'll ensure that at least one recipient is generated
    ProposalNotificationGenerator spyGenerator = Mockito.spy(proposalNotificationGenerator);

    Urn recipientUrn = UrnUtils.getUrn("urn:li:corpuser:recipient");

    NotificationRecipient mockRecipient = new NotificationRecipient();
    mockRecipient.setActor(recipientUrn);
    mockRecipient.setId("test@test.com");
    mockRecipient.setType(NotificationRecipientType.EMAIL);
    mockRecipient.setOrigin(NotificationRecipientOriginType.ACTOR_NOTIFICATION);

    doReturn(Collections.singletonList(mockRecipient))
        .when(spyGenerator)
        .buildRecipients(
            any(),
            eq(NotificationScenarioType.PROPOSAL_STATUS_CHANGE),
            eq(actionRequestUrn),
            any(),
            any(),
            any());

    spyGenerator.generateUpdatedProposalNotifications(
        actionRequestUrn, info, newStatus, new AuditStamp().setActor(TEST_USER_CREATOR_URN));

    // Capture
    verify(mockEventProducer, atLeast(1))
        .producePlatformEvent(
            Mockito.eq(NOTIFICATION_REQUEST_EVENT_NAME),
            Mockito.eq(null),
            platformEventArgumentCaptor.capture());

    PlatformEvent platformEvent = platformEventArgumentCaptor.getValue();
    assertNotNull(platformEvent, "PlatformEvent should be produced");

    NotificationRequest notificationRequest =
        GenericRecordUtils.deserializePayload(
            platformEvent.getPayload().getValue(), NotificationRequest.class);

    // Check the templateParams
    Map<String, String> templateParams = notificationRequest.getMessage().getParameters();

    // We expect "action" => "accepted"
    assertEquals(templateParams.get("operation"), "create");
    assertEquals(templateParams.get("entityName"), "test term name");
    assertEquals(templateParams.get("entityType"), "Glossary Term");
    assertEquals(templateParams.get("actorUrn"), TEST_USER_CREATOR_URN.toString());
    assertEquals(templateParams.get("actorName"), "Test Creator Name");
    assertEquals(templateParams.get("action"), "accepted");
  }

  @Test
  public void testTemplateParamsArePopulatedForCreateGlossaryNodeProposal() {
    Urn actionRequestUrn = UrnUtils.getUrn("urn:li:actionRequest:444");
    ActionRequestInfo info = new ActionRequestInfo();
    info.setType(AcrylConstants.ACTION_REQUEST_TYPE_CREATE_GLOSSARY_NODE_PROPOSAL);
    Urn parentNodeUrn = UrnUtils.getUrn("urn:li:glossaryNode:123");
    info.setParams(
        new ActionRequestParams()
            .setCreateGlossaryNodeProposal(
                new CreateGlossaryNodeProposal()
                    .setName("test group name")
                    .setDescription("test group description")
                    .setParentNode(parentNodeUrn)));

    // We'll ensure that at least one recipient is generated
    ProposalNotificationGenerator spyGenerator = Mockito.spy(proposalNotificationGenerator);

    Urn recipientUrn = UrnUtils.getUrn("urn:li:corpuser:recipient");

    NotificationRecipient mockRecipient = new NotificationRecipient();
    mockRecipient.setActor(recipientUrn);
    mockRecipient.setId("test@test.com");
    mockRecipient.setType(NotificationRecipientType.EMAIL);
    mockRecipient.setOrigin(NotificationRecipientOriginType.ACTOR_NOTIFICATION);

    doReturn(Collections.singletonList(mockRecipient))
        .when(spyGenerator)
        .buildRecipients(
            any(),
            eq(NotificationScenarioType.NEW_PROPOSAL),
            eq(actionRequestUrn),
            any(),
            any(),
            any());

    // Trigger
    spyGenerator.generateNewProposalNotifications(
        actionRequestUrn, info, new AuditStamp().setActor(TEST_USER_CREATOR_URN));

    // Capture
    verify(mockEventProducer, atLeast(1))
        .producePlatformEvent(
            Mockito.eq(NOTIFICATION_REQUEST_EVENT_NAME),
            Mockito.eq(null),
            platformEventArgumentCaptor.capture());

    // We can examine the captured NotificationRequest, check template, recipients, etc.
    PlatformEvent platformEvent = platformEventArgumentCaptor.getValue();
    assertNotNull(platformEvent, "PlatformEvent should be produced");

    NotificationRequest notificationRequest =
        GenericRecordUtils.deserializePayload(
            platformEvent.getPayload().getValue(), NotificationRequest.class);
    assertEquals(
        notificationRequest.getMessage().getTemplate().toString(),
        NotificationTemplateType.BROADCAST_NEW_PROPOSAL.name());

    // Check the templateParams
    Map<String, String> templateParams = notificationRequest.getMessage().getParameters();
    // For the scenario type TAG_PROPOSAL -> operation=add, etc.
    assertEquals(templateParams.get("operation"), "create");
    assertEquals(templateParams.get("entityName"), "test group name");
    assertEquals(templateParams.get("entityType"), "Glossary Term Group");
    assertEquals(templateParams.get("actorUrn"), TEST_USER_CREATOR_URN.toString());
    assertEquals(templateParams.get("actorName"), "Test Creator Name");
  }

  @Test
  public void testTemplateParamsArePopulatedForCreateGlossaryNodeProposalStatusChange() {
    Urn actionRequestUrn = UrnUtils.getUrn("urn:li:actionRequest:555");
    ActionRequestInfo info = new ActionRequestInfo();
    info.setType(AcrylConstants.ACTION_REQUEST_TYPE_CREATE_GLOSSARY_NODE_PROPOSAL);
    Urn parentNodeUrn = UrnUtils.getUrn("urn:li:glossaryNode:123");
    info.setParams(
        new ActionRequestParams()
            .setCreateGlossaryNodeProposal(
                new CreateGlossaryNodeProposal()
                    .setName("test group name")
                    .setDescription("test group description")
                    .setParentNode(parentNodeUrn)));

    ActionRequestStatus newStatus = new ActionRequestStatus();
    newStatus.setResult(AcrylConstants.ACTION_REQUEST_RESULT_ACCEPTED);

    // We'll ensure that at least one recipient is generated
    ProposalNotificationGenerator spyGenerator = Mockito.spy(proposalNotificationGenerator);

    Urn recipientUrn = UrnUtils.getUrn("urn:li:corpuser:recipient");

    NotificationRecipient mockRecipient = new NotificationRecipient();
    mockRecipient.setActor(recipientUrn);
    mockRecipient.setId("test@test.com");
    mockRecipient.setType(NotificationRecipientType.EMAIL);
    mockRecipient.setOrigin(NotificationRecipientOriginType.ACTOR_NOTIFICATION);

    doReturn(Collections.singletonList(mockRecipient))
        .when(spyGenerator)
        .buildRecipients(
            any(),
            eq(NotificationScenarioType.PROPOSAL_STATUS_CHANGE),
            eq(actionRequestUrn),
            any(),
            any(),
            any());

    spyGenerator.generateUpdatedProposalNotifications(
        actionRequestUrn, info, newStatus, new AuditStamp().setActor(TEST_USER_CREATOR_URN));

    // Capture
    verify(mockEventProducer, atLeast(1))
        .producePlatformEvent(
            Mockito.eq(NOTIFICATION_REQUEST_EVENT_NAME),
            Mockito.eq(null),
            platformEventArgumentCaptor.capture());

    PlatformEvent platformEvent = platformEventArgumentCaptor.getValue();
    assertNotNull(platformEvent, "PlatformEvent should be produced");

    NotificationRequest notificationRequest =
        GenericRecordUtils.deserializePayload(
            platformEvent.getPayload().getValue(), NotificationRequest.class);

    // Check the templateParams
    Map<String, String> templateParams = notificationRequest.getMessage().getParameters();

    // We expect "action" => "accepted"
    assertEquals(templateParams.get("operation"), "create");
    assertEquals(templateParams.get("entityName"), "test group name");
    assertEquals(templateParams.get("entityType"), "Glossary Term Group");
    assertEquals(templateParams.get("actorUrn"), TEST_USER_CREATOR_URN.toString());
    assertEquals(templateParams.get("actorName"), "Test Creator Name");
    assertEquals(templateParams.get("action"), "accepted");
  }

  @Test
  public void testTemplateParamsArePopulatedForNewAssetStructuredPropertyProposal() {
    Urn actionRequestUrn = UrnUtils.getUrn("urn:li:actionRequest:444");
    ActionRequestInfo info = new ActionRequestInfo();
    info.setType(AcrylConstants.ACTION_REQUEST_TYPE_STRUCTURED_PROPERTY_PROPOSAL);
    info.setParams(
        new ActionRequestParams()
            .setStructuredPropertyProposal(
                new StructuredPropertyProposal()
                    .setStructuredPropertyValues(
                        new StructuredPropertyValueAssignmentArray(
                            Collections.singletonList(
                                new StructuredPropertyValueAssignment()
                                    .setPropertyUrn(TEST_STRUCTURED_PROPERTY_URN)
                                    .setValues(
                                        new PrimitivePropertyValueArray(
                                            Collections.singletonList(
                                                PrimitivePropertyValue.create("value")))))))));
    info.setResource(TEST_DATASET_URN.toString());

    // We'll ensure that at least one recipient is generated
    ProposalNotificationGenerator spyGenerator = Mockito.spy(proposalNotificationGenerator);

    Urn recipientUrn = UrnUtils.getUrn("urn:li:corpuser:recipient");

    NotificationRecipient mockRecipient = new NotificationRecipient();
    mockRecipient.setActor(recipientUrn);
    mockRecipient.setId("test@test.com");
    mockRecipient.setType(NotificationRecipientType.EMAIL);
    mockRecipient.setOrigin(NotificationRecipientOriginType.ACTOR_NOTIFICATION);

    doReturn(Collections.singletonList(mockRecipient))
        .when(spyGenerator)
        .buildRecipients(
            any(),
            eq(NotificationScenarioType.NEW_PROPOSAL),
            eq(actionRequestUrn),
            any(),
            any(),
            any());

    // Trigger
    spyGenerator.generateNewProposalNotifications(
        actionRequestUrn, info, new AuditStamp().setActor(TEST_USER_CREATOR_URN));

    // Capture
    verify(mockEventProducer, atLeast(1))
        .producePlatformEvent(
            Mockito.eq(NOTIFICATION_REQUEST_EVENT_NAME),
            Mockito.eq(null),
            platformEventArgumentCaptor.capture());

    // We can examine the captured NotificationRequest, check template, recipients, etc.
    PlatformEvent platformEvent = platformEventArgumentCaptor.getValue();
    assertNotNull(platformEvent, "PlatformEvent should be produced");

    NotificationRequest notificationRequest =
        GenericRecordUtils.deserializePayload(
            platformEvent.getPayload().getValue(), NotificationRequest.class);
    assertEquals(
        notificationRequest.getMessage().getTemplate().toString(),
        NotificationTemplateType.BROADCAST_NEW_PROPOSAL.name());

    // Check the templateParams
    Map<String, String> templateParams = notificationRequest.getMessage().getParameters();
    assertEquals(templateParams.get("operation"), "add");
    assertEquals(templateParams.get("modifierType"), "Structured Properties");
    assertEquals(templateParams.get("modifierNames"), "[\"Test Property Name\"]");
    assertEquals(templateParams.get("modifierPaths"), "[\"/structured-properties\"]");
    assertEquals(templateParams.get("entityName"), "Test Dataset Name");
    assertEquals(templateParams.get("entityType"), "Dataset");
    assertEquals(
        templateParams.get("entityPath"),
        String.format("/dataset/%s", urlEncode(TEST_DATASET_URN)));
    assertEquals(templateParams.get("actorUrn"), TEST_USER_CREATOR_URN.toString());
    assertEquals(templateParams.get("actorName"), "Test Creator Name");
    assertEquals(
        templateParams.get("context"),
        "{\"structuredPropertyValues\":[{\"propertyUrn\":\"urn:li:structuredProperty:test\",\"values\":[{\"string\":\"value\"}]}]}");
  }

  @Test
  public void testTemplateParamsArePopulatedForUpdatedAssetStructuredPropertyProposal() {
    Urn actionRequestUrn = UrnUtils.getUrn("urn:li:actionRequest:555");
    ActionRequestInfo info = new ActionRequestInfo();
    info.setType(AcrylConstants.ACTION_REQUEST_TYPE_STRUCTURED_PROPERTY_PROPOSAL);
    info.setParams(
        new ActionRequestParams()
            .setStructuredPropertyProposal(
                new StructuredPropertyProposal()
                    .setStructuredPropertyValues(
                        new StructuredPropertyValueAssignmentArray(
                            Collections.singletonList(
                                new StructuredPropertyValueAssignment()
                                    .setPropertyUrn(TEST_STRUCTURED_PROPERTY_URN)
                                    .setValues(
                                        new PrimitivePropertyValueArray(
                                            Collections.singletonList(
                                                PrimitivePropertyValue.create("value")))))))));
    info.setResource(TEST_DATASET_URN.toString());

    ActionRequestStatus newStatus = new ActionRequestStatus();
    newStatus.setResult(AcrylConstants.ACTION_REQUEST_RESULT_ACCEPTED);

    // We'll ensure that at least one recipient is generated
    ProposalNotificationGenerator spyGenerator = Mockito.spy(proposalNotificationGenerator);

    Urn recipientUrn = UrnUtils.getUrn("urn:li:corpuser:recipient");

    NotificationRecipient mockRecipient = new NotificationRecipient();
    mockRecipient.setActor(recipientUrn);
    mockRecipient.setId("test@test.com");
    mockRecipient.setType(NotificationRecipientType.EMAIL);
    mockRecipient.setOrigin(NotificationRecipientOriginType.ACTOR_NOTIFICATION);

    doReturn(Collections.singletonList(mockRecipient))
        .when(spyGenerator)
        .buildRecipients(
            any(),
            eq(NotificationScenarioType.PROPOSAL_STATUS_CHANGE),
            eq(actionRequestUrn),
            any(),
            any(),
            any());

    spyGenerator.generateUpdatedProposalNotifications(
        actionRequestUrn, info, newStatus, new AuditStamp().setActor(TEST_USER_CREATOR_URN));

    // Capture
    verify(mockEventProducer, atLeast(1))
        .producePlatformEvent(
            Mockito.eq(NOTIFICATION_REQUEST_EVENT_NAME),
            Mockito.eq(null),
            platformEventArgumentCaptor.capture());

    PlatformEvent platformEvent = platformEventArgumentCaptor.getValue();
    assertNotNull(platformEvent, "PlatformEvent should be produced");

    NotificationRequest notificationRequest =
        GenericRecordUtils.deserializePayload(
            platformEvent.getPayload().getValue(), NotificationRequest.class);

    // Check the templateParams
    Map<String, String> templateParams = notificationRequest.getMessage().getParameters();

    // We expect "action" => "accepted"
    assertEquals(templateParams.get("operation"), "add");
    assertEquals(templateParams.get("modifierType"), "Structured Properties");
    assertEquals(templateParams.get("modifierNames"), "[\"Test Property Name\"]");
    assertEquals(templateParams.get("modifierPaths"), "[\"/structured-properties\"]");
    assertEquals(templateParams.get("entityName"), "Test Dataset Name");
    assertEquals(templateParams.get("entityType"), "Dataset");
    assertEquals(
        templateParams.get("entityPath"),
        String.format("/dataset/%s", urlEncode(TEST_DATASET_URN)));
    assertEquals(templateParams.get("actorUrn"), TEST_USER_CREATOR_URN.toString());
    assertEquals(templateParams.get("actorName"), "Test Creator Name");
    assertEquals(templateParams.get("action"), "accepted");
    assertEquals(
        templateParams.get("context"),
        "{\"structuredPropertyValues\":[{\"propertyUrn\":\"urn:li:structuredProperty:test\",\"values\":[{\"string\":\"value\"}]}]}");
  }

  @Test
  public void testTemplateParamsArePopulatedForNewSchemaFieldStructuredPropertyProposal() {
    Urn actionRequestUrn = UrnUtils.getUrn("urn:li:actionRequest:444");
    ActionRequestInfo info = new ActionRequestInfo();
    info.setType(AcrylConstants.ACTION_REQUEST_TYPE_STRUCTURED_PROPERTY_PROPOSAL);
    info.setParams(
        new ActionRequestParams()
            .setStructuredPropertyProposal(
                new StructuredPropertyProposal()
                    .setStructuredPropertyValues(
                        new StructuredPropertyValueAssignmentArray(
                            Collections.singletonList(
                                new StructuredPropertyValueAssignment()
                                    .setPropertyUrn(TEST_STRUCTURED_PROPERTY_URN)
                                    .setValues(
                                        new PrimitivePropertyValueArray(
                                            Collections.singletonList(
                                                PrimitivePropertyValue.create("value")))))))));
    info.setResource(TEST_DATASET_URN.toString());
    info.setSubResourceType(SubResourceType.DATASET_FIELD.toString());
    info.setSubResource("testField");

    // We'll ensure that at least one recipient is generated
    ProposalNotificationGenerator spyGenerator = Mockito.spy(proposalNotificationGenerator);

    Urn recipientUrn = UrnUtils.getUrn("urn:li:corpuser:recipient");

    NotificationRecipient mockRecipient = new NotificationRecipient();
    mockRecipient.setActor(recipientUrn);
    mockRecipient.setId("test@test.com");
    mockRecipient.setType(NotificationRecipientType.EMAIL);
    mockRecipient.setOrigin(NotificationRecipientOriginType.ACTOR_NOTIFICATION);

    doReturn(Collections.singletonList(mockRecipient))
        .when(spyGenerator)
        .buildRecipients(
            any(),
            eq(NotificationScenarioType.NEW_PROPOSAL),
            eq(actionRequestUrn),
            any(),
            any(),
            any());

    // Trigger
    spyGenerator.generateNewProposalNotifications(
        actionRequestUrn, info, new AuditStamp().setActor(TEST_USER_CREATOR_URN));

    // Capture
    verify(mockEventProducer, atLeast(1))
        .producePlatformEvent(
            Mockito.eq(NOTIFICATION_REQUEST_EVENT_NAME),
            Mockito.eq(null),
            platformEventArgumentCaptor.capture());

    // We can examine the captured NotificationRequest, check template, recipients, etc.
    PlatformEvent platformEvent = platformEventArgumentCaptor.getValue();
    assertNotNull(platformEvent, "PlatformEvent should be produced");

    NotificationRequest notificationRequest =
        GenericRecordUtils.deserializePayload(
            platformEvent.getPayload().getValue(), NotificationRequest.class);
    assertEquals(
        notificationRequest.getMessage().getTemplate().toString(),
        NotificationTemplateType.BROADCAST_NEW_PROPOSAL.name());

    // Check the templateParams
    Map<String, String> templateParams = notificationRequest.getMessage().getParameters();
    assertEquals(templateParams.get("operation"), "add");
    assertEquals(templateParams.get("modifierType"), "Structured Properties");
    assertEquals(templateParams.get("modifierNames"), "[\"Test Property Name\"]");
    assertEquals(templateParams.get("modifierPaths"), "[\"/structured-properties\"]");
    assertEquals(templateParams.get("entityName"), "Test Dataset Name");
    assertEquals(templateParams.get("entityType"), "Dataset");
    assertEquals(
        templateParams.get("entityPath"),
        String.format("/dataset/%s", urlEncode(TEST_DATASET_URN)));
    assertEquals(templateParams.get("actorUrn"), TEST_USER_CREATOR_URN.toString());
    assertEquals(templateParams.get("actorName"), "Test Creator Name");
    assertEquals(
        templateParams.get("context"),
        "{\"structuredPropertyValues\":[{\"propertyUrn\":\"urn:li:structuredProperty:test\",\"values\":[{\"string\":\"value\"}]}]}");
    assertEquals(templateParams.get("subResourceType"), "DATASET_FIELD");
    assertEquals(templateParams.get("subResource"), "testField");
  }

  @Test
  public void testTemplateParamsArePopulatedForUpdatedSchemaFieldStructuredPropertyProposal() {
    Urn actionRequestUrn = UrnUtils.getUrn("urn:li:actionRequest:555");
    ActionRequestInfo info = new ActionRequestInfo();
    info.setType(AcrylConstants.ACTION_REQUEST_TYPE_STRUCTURED_PROPERTY_PROPOSAL);
    info.setParams(
        new ActionRequestParams()
            .setStructuredPropertyProposal(
                new StructuredPropertyProposal()
                    .setStructuredPropertyValues(
                        new StructuredPropertyValueAssignmentArray(
                            Collections.singletonList(
                                new StructuredPropertyValueAssignment()
                                    .setPropertyUrn(TEST_STRUCTURED_PROPERTY_URN)
                                    .setValues(
                                        new PrimitivePropertyValueArray(
                                            Collections.singletonList(
                                                PrimitivePropertyValue.create("value")))))))));
    info.setResource(TEST_DATASET_URN.toString());
    info.setSubResourceType(SubResourceType.DATASET_FIELD.toString());
    info.setSubResource("testField");

    ActionRequestStatus newStatus = new ActionRequestStatus();
    newStatus.setResult(AcrylConstants.ACTION_REQUEST_RESULT_ACCEPTED);

    // We'll ensure that at least one recipient is generated
    ProposalNotificationGenerator spyGenerator = Mockito.spy(proposalNotificationGenerator);

    Urn recipientUrn = UrnUtils.getUrn("urn:li:corpuser:recipient");

    NotificationRecipient mockRecipient = new NotificationRecipient();
    mockRecipient.setActor(recipientUrn);
    mockRecipient.setId("test@test.com");
    mockRecipient.setType(NotificationRecipientType.EMAIL);
    mockRecipient.setOrigin(NotificationRecipientOriginType.ACTOR_NOTIFICATION);

    doReturn(Collections.singletonList(mockRecipient))
        .when(spyGenerator)
        .buildRecipients(
            any(),
            eq(NotificationScenarioType.PROPOSAL_STATUS_CHANGE),
            eq(actionRequestUrn),
            any(),
            any(),
            any());

    spyGenerator.generateUpdatedProposalNotifications(
        actionRequestUrn, info, newStatus, new AuditStamp().setActor(TEST_USER_CREATOR_URN));

    // Capture
    verify(mockEventProducer, atLeast(1))
        .producePlatformEvent(
            Mockito.eq(NOTIFICATION_REQUEST_EVENT_NAME),
            Mockito.eq(null),
            platformEventArgumentCaptor.capture());

    PlatformEvent platformEvent = platformEventArgumentCaptor.getValue();
    assertNotNull(platformEvent, "PlatformEvent should be produced");

    NotificationRequest notificationRequest =
        GenericRecordUtils.deserializePayload(
            platformEvent.getPayload().getValue(), NotificationRequest.class);

    // Check the templateParams
    Map<String, String> templateParams = notificationRequest.getMessage().getParameters();

    // We expect "action" => "accepted"
    assertEquals(templateParams.get("operation"), "add");
    assertEquals(templateParams.get("modifierType"), "Structured Properties");
    assertEquals(templateParams.get("modifierNames"), "[\"Test Property Name\"]");
    assertEquals(templateParams.get("modifierPaths"), "[\"/structured-properties\"]");
    assertEquals(templateParams.get("entityName"), "Test Dataset Name");
    assertEquals(templateParams.get("entityType"), "Dataset");
    assertEquals(
        templateParams.get("entityPath"),
        String.format("/dataset/%s", urlEncode(TEST_DATASET_URN)));
    assertEquals(templateParams.get("actorUrn"), TEST_USER_CREATOR_URN.toString());
    assertEquals(templateParams.get("actorName"), "Test Creator Name");
    assertEquals(templateParams.get("action"), "accepted");
    assertEquals(
        templateParams.get("context"),
        "{\"structuredPropertyValues\":[{\"propertyUrn\":\"urn:li:structuredProperty:test\",\"values\":[{\"string\":\"value\"}]}]}");
    assertEquals(templateParams.get("subResourceType"), "DATASET_FIELD");
    assertEquals(templateParams.get("subResource"), "testField");
  }

  @Test
  public void testTemplateParamsArePopulatedForNewDomainProposal() {
    Urn actionRequestUrn = UrnUtils.getUrn("urn:li:actionRequest:444");
    ActionRequestInfo info = new ActionRequestInfo();
    info.setType(AcrylConstants.ACTION_REQUEST_TYPE_DOMAIN_PROPOSAL);
    info.setParams(
        new ActionRequestParams()
            .setDomainProposal(
                new DomainProposal()
                    .setDomains(
                        new UrnArray(
                            Collections.singletonList(
                                UrnUtils.getUrn(TEST_DOMAIN_URN.toString()))))));
    info.setResource(TEST_DATASET_URN.toString());

    // We'll ensure that at least one recipient is generated
    ProposalNotificationGenerator spyGenerator = Mockito.spy(proposalNotificationGenerator);

    Urn recipientUrn = UrnUtils.getUrn("urn:li:corpuser:recipient");

    NotificationRecipient mockRecipient = new NotificationRecipient();
    mockRecipient.setActor(recipientUrn);
    mockRecipient.setId("test@test.com");
    mockRecipient.setType(NotificationRecipientType.EMAIL);
    mockRecipient.setOrigin(NotificationRecipientOriginType.ACTOR_NOTIFICATION);

    doReturn(Collections.singletonList(mockRecipient))
        .when(spyGenerator)
        .buildRecipients(
            any(),
            eq(NotificationScenarioType.NEW_PROPOSAL),
            eq(actionRequestUrn),
            any(),
            any(),
            any());

    // Trigger
    spyGenerator.generateNewProposalNotifications(
        actionRequestUrn, info, new AuditStamp().setActor(TEST_USER_CREATOR_URN));

    // Capture
    verify(mockEventProducer, atLeast(1))
        .producePlatformEvent(
            Mockito.eq(NOTIFICATION_REQUEST_EVENT_NAME),
            Mockito.eq(null),
            platformEventArgumentCaptor.capture());

    // We can examine the captured NotificationRequest, check template, recipients, etc.
    PlatformEvent platformEvent = platformEventArgumentCaptor.getValue();
    assertNotNull(platformEvent, "PlatformEvent should be produced");

    NotificationRequest notificationRequest =
        GenericRecordUtils.deserializePayload(
            platformEvent.getPayload().getValue(), NotificationRequest.class);
    assertEquals(
        notificationRequest.getMessage().getTemplate().toString(),
        NotificationTemplateType.BROADCAST_NEW_PROPOSAL.name());

    // Check the templateParams
    Map<String, String> templateParams = notificationRequest.getMessage().getParameters();
    assertEquals(templateParams.get("operation"), "add");
    assertEquals(templateParams.get("modifierType"), "Domain");
    assertEquals(templateParams.get("modifierNames"), "[\"Test Domain Name\"]");
    assertEquals(
        templateParams.get("modifierPaths"),
        String.format("[\"/domain/%s\"]", urlEncode(TEST_DOMAIN_URN)));
    assertEquals(templateParams.get("entityName"), "Test Dataset Name");
    assertEquals(templateParams.get("entityType"), "Dataset");
    assertEquals(
        templateParams.get("entityPath"),
        String.format("/dataset/%s", urlEncode(TEST_DATASET_URN)));
    assertEquals(templateParams.get("actorUrn"), TEST_USER_CREATOR_URN.toString());
    assertEquals(templateParams.get("actorName"), "Test Creator Name");
  }

  @Test
  public void testTemplateParamsArePopulatedForUpdatedDomainProposal() {
    Urn actionRequestUrn = UrnUtils.getUrn("urn:li:actionRequest:555");
    ActionRequestInfo info = new ActionRequestInfo();
    info.setType(AcrylConstants.ACTION_REQUEST_TYPE_DOMAIN_PROPOSAL);
    info.setParams(
        new ActionRequestParams()
            .setDomainProposal(
                new DomainProposal()
                    .setDomains(
                        new UrnArray(
                            Collections.singletonList(
                                UrnUtils.getUrn(TEST_DOMAIN_URN.toString()))))));
    info.setResource(TEST_DATASET_URN.toString());

    ActionRequestStatus newStatus = new ActionRequestStatus();
    newStatus.setResult(AcrylConstants.ACTION_REQUEST_RESULT_ACCEPTED);

    // We'll ensure that at least one recipient is generated
    ProposalNotificationGenerator spyGenerator = Mockito.spy(proposalNotificationGenerator);

    Urn recipientUrn = UrnUtils.getUrn("urn:li:corpuser:recipient");

    NotificationRecipient mockRecipient = new NotificationRecipient();
    mockRecipient.setActor(recipientUrn);
    mockRecipient.setId("test@test.com");
    mockRecipient.setType(NotificationRecipientType.EMAIL);
    mockRecipient.setOrigin(NotificationRecipientOriginType.ACTOR_NOTIFICATION);

    doReturn(Collections.singletonList(mockRecipient))
        .when(spyGenerator)
        .buildRecipients(
            any(),
            eq(NotificationScenarioType.PROPOSAL_STATUS_CHANGE),
            eq(actionRequestUrn),
            any(),
            any(),
            any());

    spyGenerator.generateUpdatedProposalNotifications(
        actionRequestUrn, info, newStatus, new AuditStamp().setActor(TEST_USER_CREATOR_URN));

    // Capture
    verify(mockEventProducer, atLeast(1))
        .producePlatformEvent(
            Mockito.eq(NOTIFICATION_REQUEST_EVENT_NAME),
            Mockito.eq(null),
            platformEventArgumentCaptor.capture());

    PlatformEvent platformEvent = platformEventArgumentCaptor.getValue();
    assertNotNull(platformEvent, "PlatformEvent should be produced");

    NotificationRequest notificationRequest =
        GenericRecordUtils.deserializePayload(
            platformEvent.getPayload().getValue(), NotificationRequest.class);

    // Check the templateParams
    Map<String, String> templateParams = notificationRequest.getMessage().getParameters();

    // We expect "action" => "accepted"
    assertEquals(templateParams.get("operation"), "add");
    assertEquals(templateParams.get("modifierType"), "Domain");
    assertEquals(templateParams.get("modifierNames"), "[\"Test Domain Name\"]");
    assertEquals(
        templateParams.get("modifierPaths"),
        String.format("[\"/domain/%s\"]", urlEncode(TEST_DOMAIN_URN)));
    assertEquals(templateParams.get("entityName"), "Test Dataset Name");
    assertEquals(templateParams.get("entityType"), "Dataset");
    assertEquals(
        templateParams.get("entityPath"),
        String.format("/dataset/%s", urlEncode(TEST_DATASET_URN)));
    assertEquals(templateParams.get("actorUrn"), TEST_USER_CREATOR_URN.toString());
    assertEquals(templateParams.get("actorName"), "Test Creator Name");
    assertEquals(templateParams.get("action"), "accepted");
  }

  @Test
  public void testTemplateParamsArePopulatedForNewOwnerProposal() {
    Urn actionRequestUrn = UrnUtils.getUrn("urn:li:actionRequest:444");
    ActionRequestInfo info = new ActionRequestInfo();
    info.setType(AcrylConstants.ACTION_REQUEST_TYPE_OWNER_PROPOSAL);
    info.setParams(
        new ActionRequestParams()
            .setOwnerProposal(
                new OwnerProposal()
                    .setOwners(
                        new OwnerArray(
                            ImmutableList.of(
                                new Owner()
                                    .setOwner(TEST_USER_OWNER_URN)
                                    .setType(OwnershipType.TECHNICAL_OWNER)
                                    .setTypeUrn(TEST_OWNERSHIP_TYPE_URN)
                                    .setSource(
                                        new OwnershipSource().setType(OwnershipSourceType.MANUAL)),
                                new Owner()
                                    .setOwner(TEST_GROUP_OWNER_URN)
                                    .setType(OwnershipType.BUSINESS_OWNER)
                                    .setTypeUrn(TEST_OWNERSHIP_TYPE_URN)
                                    .setSource(
                                        new OwnershipSource()
                                            .setType(OwnershipSourceType.MANUAL)))))));
    info.setResource(TEST_DATASET_URN.toString());

    // We'll ensure that at least one recipient is generated
    ProposalNotificationGenerator spyGenerator = Mockito.spy(proposalNotificationGenerator);

    Urn recipientUrn = UrnUtils.getUrn("urn:li:corpuser:recipient");

    NotificationRecipient mockRecipient = new NotificationRecipient();
    mockRecipient.setActor(recipientUrn);
    mockRecipient.setId("test@test.com");
    mockRecipient.setType(NotificationRecipientType.EMAIL);
    mockRecipient.setOrigin(NotificationRecipientOriginType.ACTOR_NOTIFICATION);

    doReturn(Collections.singletonList(mockRecipient))
        .when(spyGenerator)
        .buildRecipients(
            any(),
            eq(NotificationScenarioType.NEW_PROPOSAL),
            eq(actionRequestUrn),
            any(),
            any(),
            any());

    // Trigger
    spyGenerator.generateNewProposalNotifications(
        actionRequestUrn, info, new AuditStamp().setActor(TEST_USER_CREATOR_URN));

    // Capture
    verify(mockEventProducer, atLeast(1))
        .producePlatformEvent(
            Mockito.eq(NOTIFICATION_REQUEST_EVENT_NAME),
            Mockito.eq(null),
            platformEventArgumentCaptor.capture());

    // We can examine the captured NotificationRequest, check template, recipients, etc.
    PlatformEvent platformEvent = platformEventArgumentCaptor.getValue();
    assertNotNull(platformEvent, "PlatformEvent should be produced");

    NotificationRequest notificationRequest =
        GenericRecordUtils.deserializePayload(
            platformEvent.getPayload().getValue(), NotificationRequest.class);
    assertEquals(
        notificationRequest.getMessage().getTemplate().toString(),
        NotificationTemplateType.BROADCAST_NEW_PROPOSAL.name());

    // Check the templateParams
    Map<String, String> templateParams = notificationRequest.getMessage().getParameters();
    assertEquals(templateParams.get("operation"), "add");
    assertEquals(templateParams.get("modifierType"), "Owner(s)");
    assertEquals(
        templateParams.get("modifierNames"),
        "[\"Test User Owner Name\",\"Test Group Owner Name\"]");
    assertEquals(
        templateParams.get("modifierPaths"),
        String.format(
            "[\"/user/%s\",\"/group/%s\"]",
            urlEncode(TEST_USER_OWNER_URN), urlEncode(TEST_GROUP_OWNER_URN)));
    assertEquals(templateParams.get("entityName"), "Test Dataset Name");
    assertEquals(templateParams.get("entityType"), "Dataset");
    assertEquals(
        templateParams.get("entityPath"),
        String.format("/dataset/%s", urlEncode(TEST_DATASET_URN)));
    assertEquals(templateParams.get("actorUrn"), TEST_USER_CREATOR_URN.toString());
    assertEquals(templateParams.get("actorName"), "Test Creator Name");
    assertEquals(
        templateParams.get("context"),
        "{\"owners\":[{\"owner\":\"urn:li:corpuser:testUser\",\"typeUrn\":\"urn:li:ownershipType:test\",\"source\":{\"type\":\"MANUAL\"},\"type\":\"TECHNICAL_OWNER\"},{\"owner\":\"urn:li:corpGroup:testGroup\",\"typeUrn\":\"urn:li:ownershipType:test\",\"source\":{\"type\":\"MANUAL\"},\"type\":\"BUSINESS_OWNER\"}]}");
  }

  @Test
  public void testTemplateParamsArePopulatedForUpdatedOwnerProposal() {
    Urn actionRequestUrn = UrnUtils.getUrn("urn:li:actionRequest:555");
    ActionRequestInfo info = new ActionRequestInfo();
    info.setType(AcrylConstants.ACTION_REQUEST_TYPE_OWNER_PROPOSAL);
    info.setParams(
        new ActionRequestParams()
            .setOwnerProposal(
                new OwnerProposal()
                    .setOwners(
                        new OwnerArray(
                            ImmutableList.of(
                                new Owner()
                                    .setOwner(TEST_USER_OWNER_URN)
                                    .setType(OwnershipType.TECHNICAL_OWNER)
                                    .setTypeUrn(TEST_OWNERSHIP_TYPE_URN)
                                    .setSource(
                                        new OwnershipSource().setType(OwnershipSourceType.MANUAL)),
                                new Owner()
                                    .setOwner(TEST_GROUP_OWNER_URN)
                                    .setType(OwnershipType.BUSINESS_OWNER)
                                    .setTypeUrn(TEST_OWNERSHIP_TYPE_URN)
                                    .setSource(
                                        new OwnershipSource()
                                            .setType(OwnershipSourceType.MANUAL)))))));
    info.setResource(TEST_DATASET_URN.toString());

    ActionRequestStatus newStatus = new ActionRequestStatus();
    newStatus.setResult(AcrylConstants.ACTION_REQUEST_RESULT_ACCEPTED);

    // We'll ensure that at least one recipient is generated
    ProposalNotificationGenerator spyGenerator = Mockito.spy(proposalNotificationGenerator);

    Urn recipientUrn = UrnUtils.getUrn("urn:li:corpuser:recipient");

    NotificationRecipient mockRecipient = new NotificationRecipient();
    mockRecipient.setActor(recipientUrn);
    mockRecipient.setId("test@test.com");
    mockRecipient.setType(NotificationRecipientType.EMAIL);
    mockRecipient.setOrigin(NotificationRecipientOriginType.ACTOR_NOTIFICATION);

    doReturn(Collections.singletonList(mockRecipient))
        .when(spyGenerator)
        .buildRecipients(
            any(),
            eq(NotificationScenarioType.PROPOSAL_STATUS_CHANGE),
            eq(actionRequestUrn),
            any(),
            any(),
            any());

    spyGenerator.generateUpdatedProposalNotifications(
        actionRequestUrn, info, newStatus, new AuditStamp().setActor(TEST_USER_CREATOR_URN));

    // Capture
    verify(mockEventProducer, atLeast(1))
        .producePlatformEvent(
            Mockito.eq(NOTIFICATION_REQUEST_EVENT_NAME),
            Mockito.eq(null),
            platformEventArgumentCaptor.capture());

    PlatformEvent platformEvent = platformEventArgumentCaptor.getValue();
    assertNotNull(platformEvent, "PlatformEvent should be produced");

    NotificationRequest notificationRequest =
        GenericRecordUtils.deserializePayload(
            platformEvent.getPayload().getValue(), NotificationRequest.class);

    // Check the templateParams
    Map<String, String> templateParams = notificationRequest.getMessage().getParameters();

    // We expect "action" => "accepted"
    assertEquals(templateParams.get("operation"), "add");
    assertEquals(templateParams.get("modifierType"), "Owner(s)");
    assertEquals(
        templateParams.get("modifierNames"),
        "[\"Test User Owner Name\",\"Test Group Owner Name\"]");
    assertEquals(
        templateParams.get("modifierPaths"),
        String.format(
            "[\"/user/%s\",\"/group/%s\"]",
            urlEncode(TEST_USER_OWNER_URN), urlEncode(TEST_GROUP_OWNER_URN)));
    assertEquals(templateParams.get("entityName"), "Test Dataset Name");
    assertEquals(templateParams.get("entityType"), "Dataset");
    assertEquals(
        templateParams.get("entityPath"),
        String.format("/dataset/%s", urlEncode(TEST_DATASET_URN)));
    assertEquals(templateParams.get("actorUrn"), TEST_USER_CREATOR_URN.toString());
    assertEquals(templateParams.get("actorName"), "Test Creator Name");
    assertEquals(templateParams.get("action"), "accepted");
    assertEquals(
        templateParams.get("context"),
        "{\"owners\":[{\"owner\":\"urn:li:corpuser:testUser\",\"typeUrn\":\"urn:li:ownershipType:test\",\"source\":{\"type\":\"MANUAL\"},\"type\":\"TECHNICAL_OWNER\"},{\"owner\":\"urn:li:corpGroup:testGroup\",\"typeUrn\":\"urn:li:ownershipType:test\",\"source\":{\"type\":\"MANUAL\"},\"type\":\"BUSINESS_OWNER\"}]}");
  }

  private String urlEncode(Urn urn) {
    return URLEncoder.encode(urn.toString(), StandardCharsets.UTF_8);
  }
}
