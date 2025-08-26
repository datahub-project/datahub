package com.linkedin.metadata.kafka.hook.notification;

import static com.linkedin.metadata.AcrylConstants.SUBSCRIPTION_ENTITY_NAME;
import static com.linkedin.metadata.Constants.*;
import static com.linkedin.metadata.kafka.hook.notification.BaseMclNotificationGenerator.MAX_DOWNSTREAMS_HOP;
import static com.linkedin.metadata.kafka.hook.notification.BaseMclNotificationGenerator.MAX_DOWNSTREAMS_TO_FETCH_OWNERSHIP;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.datahub.notification.NotificationScenarioType;
import com.datahub.notification.recipient.NotificationRecipientBuilder;
import com.datahub.notification.recipient.NotificationRecipientBuilders;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.Owner;
import com.linkedin.common.OwnerArray;
import com.linkedin.common.Ownership;
import com.linkedin.common.OwnershipSource;
import com.linkedin.common.OwnershipSourceType;
import com.linkedin.common.OwnershipType;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.DataMap;
import com.linkedin.data.template.StringMap;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.event.notification.NotificationMessage;
import com.linkedin.event.notification.NotificationRecipient;
import com.linkedin.event.notification.NotificationRecipientArray;
import com.linkedin.event.notification.NotificationRecipientOriginType;
import com.linkedin.event.notification.NotificationRecipientType;
import com.linkedin.event.notification.NotificationRequest;
import com.linkedin.event.notification.template.NotificationTemplateType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.event.EventProducer;
import com.linkedin.metadata.graph.EntityLineageResult;
import com.linkedin.metadata.graph.GraphClient;
import com.linkedin.metadata.graph.LineageDirection;
import com.linkedin.metadata.graph.LineageRelationship;
import com.linkedin.metadata.graph.LineageRelationshipArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.service.SettingsService;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import com.linkedin.mxe.MetadataChangeLog;
import com.linkedin.mxe.PlatformEvent;
import com.linkedin.subscription.EntityChangeType;
import io.datahubproject.metadata.context.ActorContext;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.SystemTelemetryContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import io.micrometer.core.instrument.Counter;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.lang.reflect.Method;
import java.util.*;
import java.util.stream.Collectors;
import org.mockito.*;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class BaseMclNotificationGeneratorTest {

  private static final Urn TEST_DATASET_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:snowflake,test,PROD)");
  private static final Urn TEST_USER_URN = UrnUtils.getUrn("urn:li:corpuser:testUser");
  private static final Urn TEST_GROUP_URN = UrnUtils.getUrn("urn:li:corpGroup:testGroup");
  private static final Urn TEST_ACTOR_URN = UrnUtils.getUrn("urn:li:corpuser:actor");

  private OperationContext operationContext;
  @Mock private ActorContext mockActorContext;
  @Mock private EventProducer mockEventProducer;
  @Mock private SystemEntityClient mockEntityClient;
  @Mock private GraphClient mockGraphClient;
  @Mock private SettingsService mockSettingsService;
  @Mock private NotificationRecipientBuilders mockRecipientBuilders;
  @Mock private MetricUtils mockMetricUtils;
  @Mock private Counter mockCounter;

  @Captor private ArgumentCaptor<PlatformEvent> platformEventCaptor;

  private TestBaseMclNotificationGenerator testGenerator;

  // Test implementation of BaseMclNotificationGenerator
  private static class TestBaseMclNotificationGenerator extends BaseMclNotificationGenerator {

    public TestBaseMclNotificationGenerator(
        OperationContext systemOpContext,
        EventProducer eventProducer,
        SystemEntityClient entityClient,
        GraphClient graphClient,
        SettingsService settingsService,
        NotificationRecipientBuilders recipientBuilders) {
      super(
          systemOpContext,
          eventProducer,
          entityClient,
          graphClient,
          settingsService,
          recipientBuilders);
    }

    @Override
    public void generate(@Nonnull MetadataChangeLog event) {
      // Test implementation
    }

    @Override
    protected boolean isEligibleForOwnerRecipients() {
      return true;
    }

    @Override
    protected boolean isEligibleForRelatedOwnerRecipients() {
      return true;
    }

    @Override
    public boolean isEligibleForSubscriberRecipients() {
      return true;
    }

    @Override
    public boolean isEligibleForCustomRecipients(@Nonnull NotificationScenarioType type) {
      return type == NotificationScenarioType.ENTITY_OWNER_CHANGE;
    }

    @Override
    protected List<NotificationRecipient> buildOwnerRecipients() {
      NotificationRecipient recipient = new NotificationRecipient();
      recipient.setId("owner@test.com");
      recipient.setType(NotificationRecipientType.EMAIL);
      recipient.setOrigin(NotificationRecipientOriginType.ACTOR_NOTIFICATION);
      return Collections.singletonList(recipient);
    }

    @Override
    protected List<NotificationRecipient> buildRelatedOwnerRecipients() {
      NotificationRecipient recipient = new NotificationRecipient();
      recipient.setId("related@test.com");
      recipient.setType(NotificationRecipientType.EMAIL);
      recipient.setOrigin(NotificationRecipientOriginType.ACTOR_NOTIFICATION);
      return Collections.singletonList(recipient);
    }

    @Override
    protected List<NotificationRecipient> buildCustomRecipients(
        @Nonnull OperationContext opContext,
        @Nonnull NotificationScenarioType type,
        @Nullable Urn entityUrn,
        @Nullable EntityChangeType changeType,
        @Nullable Urn actorUrn,
        @Nullable NotificationRecipientsGeneratorExtraContext extraContext) {
      NotificationRecipient recipient = new NotificationRecipient();
      recipient.setId("custom@test.com");
      recipient.setType(NotificationRecipientType.EMAIL);
      recipient.setOrigin(NotificationRecipientOriginType.ACTOR_NOTIFICATION);
      return Collections.singletonList(recipient);
    }
  }

  @BeforeMethod
  public void setup(Method method) throws Exception {
    MockitoAnnotations.openMocks(this);

    // Default operation context
    operationContext =
        TestOperationContexts.systemContextNoSearchAuthorization().toBuilder()
            .systemTelemetryContext(
                SystemTelemetryContext.TEST.toBuilder().metricUtils(mockMetricUtils).build())
            .build(
                new Authentication(new Actor(ActorType.USER, TEST_ACTOR_URN.getId()), ""), false);

    testGenerator =
        new TestBaseMclNotificationGenerator(
            operationContext,
            mockEventProducer,
            mockEntityClient,
            mockGraphClient,
            mockSettingsService,
            mockRecipientBuilders);

    // Set up recipient builders
    NotificationRecipientBuilder mockBuilder = mock(NotificationRecipientBuilder.class);

    NotificationRecipient globalRecipient = new NotificationRecipient();
    globalRecipient.setId("global@test.com");
    globalRecipient.setOrigin(NotificationRecipientOriginType.GLOBAL_NOTIFICATION);
    globalRecipient.setType(NotificationRecipientType.EMAIL);

    when(mockBuilder.buildGlobalRecipients(any(), any()))
        .thenReturn(Collections.singletonList(globalRecipient));

    when(mockRecipientBuilders.listBuilders()).thenReturn(Collections.singletonList(mockBuilder));
    when(mockRecipientBuilders.getBuilder(any())).thenReturn(mockBuilder);
  }

  @Test
  public void testConstructor() {
    assertNotNull(testGenerator);
  }

  @Test
  public void testIsEligibleForGlobalRecipientsNoSettings() {
    when(mockSettingsService.getGlobalSettings(operationContext)).thenReturn(null);
    assertFalse(
        testGenerator.isEligibleForGlobalRecipients(NotificationScenarioType.ENTITY_OWNER_CHANGE));
  }

  @Test
  public void testBuildRecipientsWithDeletedEntity() throws Exception {
    when(mockEntityClient.exists(any(), eq(TEST_DATASET_URN), eq(false))).thenReturn(false);

    List<NotificationRecipient> recipients =
        testGenerator.buildRecipients(
            operationContext,
            NotificationScenarioType.ENTITY_OWNER_CHANGE,
            TEST_DATASET_URN,
            EntityChangeType.OWNER_ADDED);

    assertTrue(recipients.isEmpty());
  }

  @Test
  public void testBuildGlobalRecipients() {
    List<NotificationRecipient> recipients =
        testGenerator.buildGlobalRecipients(
            operationContext, NotificationScenarioType.ENTITY_OWNER_CHANGE);

    assertEquals(recipients.size(), 1);
    assertEquals(recipients.get(0).getId(), "global@test.com");
    assertEquals(
        recipients.get(0).getOrigin(), NotificationRecipientOriginType.GLOBAL_NOTIFICATION);
  }

  @Test
  public void testGetEntityOwnership() throws Exception {
    Ownership ownership = new Ownership();
    OwnerArray owners = new OwnerArray();
    owners.add(
        new Owner()
            .setOwner(TEST_USER_URN)
            .setType(OwnershipType.TECHNICAL_OWNER)
            .setSource(new OwnershipSource().setType(OwnershipSourceType.MANUAL)));
    ownership.setOwners(owners);

    EntityResponse response = new EntityResponse();
    response.setAspects(
        new EnvelopedAspectMap(
            ImmutableMap.of(
                OWNERSHIP_ASPECT_NAME,
                new EnvelopedAspect().setValue(new Aspect(ownership.data())))));

    when(mockEntityClient.batchGetV2(
            any(),
            eq(TEST_DATASET_URN.getEntityType()),
            eq(ImmutableSet.of(TEST_DATASET_URN)),
            eq(Collections.singleton(OWNERSHIP_ASPECT_NAME))))
        .thenReturn(ImmutableMap.of(TEST_DATASET_URN, response));

    Ownership result = testGenerator.getEntityOwnership(TEST_DATASET_URN);

    assertNotNull(result);
    assertEquals(result.getOwners().size(), 1);
    assertEquals(result.getOwners().get(0).getOwner(), TEST_USER_URN);
  }

  @Test
  public void testGetEntityOwnershipNull() throws Exception {
    when(mockEntityClient.batchGetV2(any(), any(), any(), any()))
        .thenReturn(Collections.emptyMap());

    Ownership result = testGenerator.getEntityOwnership(TEST_DATASET_URN);
    assertNull(result);
  }

  @Test
  public void testGetDownstreamEntitiesException() throws Exception {
    when(mockGraphClient.getLineageEntities(any(), any(), anyInt(), anyInt(), anyInt(), any()))
        .thenThrow(new RuntimeException("Test exception"));

    Set<Urn> downstreams = testGenerator.getDownstreamEntities(TEST_DATASET_URN);

    assertTrue(downstreams.isEmpty());
  }

  @Test
  public void testBuildNotificationRequest() {
    Map<String, String> templateParams =
        ImmutableMap.of(
            "entityName", "Test Dataset",
            "actorName", "Test User");

    Set<NotificationRecipient> recipients = new HashSet<>();
    NotificationRecipient recipient = new NotificationRecipient();
    recipient.setId("test@test.com");
    recipient.setType(NotificationRecipientType.EMAIL);
    recipients.add(recipient);

    NotificationRequest request =
        testGenerator.buildNotificationRequest(
            NotificationTemplateType.OWNERSHIP_CHANGE.toString(), templateParams, recipients);

    assertNotNull(request);
    assertEquals(request.getMessage().getTemplate(), NotificationTemplateType.OWNERSHIP_CHANGE);
    assertEquals(request.getMessage().getParameters().get("entityName"), "Test Dataset");
    assertEquals(request.getRecipients().size(), 1);
  }

  @Test
  public void testCreatePlatformEvent() {
    NotificationRequest request = new NotificationRequest();
    request.setMessage(
        new NotificationMessage()
            .setTemplate(NotificationTemplateType.OWNERSHIP_CHANGE)
            .setParameters(new StringMap()));
    request.setRecipients(new NotificationRecipientArray());

    PlatformEvent event = testGenerator.createPlatformEvent(request);

    assertNotNull(event);
    assertEquals(event.getName(), NOTIFICATION_REQUEST_EVENT_NAME);
    assertNotNull(event.getPayload());
    assertNotNull(event.getHeader());
  }

  @Test
  public void testEntityExistsTrue() throws Exception {
    when(mockEntityClient.exists(any(), eq(TEST_DATASET_URN), eq(false))).thenReturn(true);

    boolean exists = testGenerator.entityExists(operationContext, TEST_DATASET_URN);
    assertTrue(exists);
  }

  @Test
  public void testEntityExistsFalse() throws Exception {
    when(mockEntityClient.exists(any(), eq(TEST_DATASET_URN), eq(false))).thenReturn(false);

    boolean exists = testGenerator.entityExists(operationContext, TEST_DATASET_URN);
    assertFalse(exists);
  }

  @Test
  public void testEntityExistsException() throws Exception {
    when(mockEntityClient.exists(any(), eq(TEST_DATASET_URN), eq(false)))
        .thenThrow(new RuntimeException("Test exception"));

    boolean exists = testGenerator.entityExists(operationContext, TEST_DATASET_URN);
    assertFalse(exists);
  }

  @Test
  public void testSendNotificationRequest() {
    NotificationRequest request = new NotificationRequest();
    request.setMessage(
        new NotificationMessage()
            .setTemplate(NotificationTemplateType.BROADCAST_ENTITY_CHANGE)
            .setParameters(new StringMap()));
    request.setRecipients(new NotificationRecipientArray());

    testGenerator.sendNotificationRequest(request);

    verify(mockEventProducer)
        .producePlatformEvent(
            eq(Constants.NOTIFICATION_REQUEST_EVENT_NAME), eq(null), any(PlatformEvent.class));
  }

  @Test
  public void testGetAspectData() throws Exception {
    DataMap dataMap = new DataMap();
    dataMap.put("test", "value");

    EntityResponse response = new EntityResponse();
    response.setAspects(
        new EnvelopedAspectMap(
            ImmutableMap.of("testAspect", new EnvelopedAspect().setValue(new Aspect(dataMap)))));

    when(mockEntityClient.getV2(
            any(),
            eq(TEST_DATASET_URN.getEntityType()),
            eq(TEST_DATASET_URN),
            eq(ImmutableSet.of("testAspect"))))
        .thenReturn(response);

    DataMap result = testGenerator.getAspectData(TEST_DATASET_URN, "testAspect");

    assertNotNull(result);
    assertEquals(result.get("test"), "value");
  }

  @Test
  public void testGetAspectDataNotFound() throws Exception {
    EntityResponse response = new EntityResponse();
    response.setAspects(new EnvelopedAspectMap());

    when(mockEntityClient.getV2(any(), any(), any(), any())).thenReturn(response);

    DataMap result = testGenerator.getAspectData(TEST_DATASET_URN, "testAspect");
    assertNull(result);
  }

  @Test
  public void testGetAspectDataException() throws Exception {
    when(mockEntityClient.getV2(any(), any(), any(), any()))
        .thenThrow(new RuntimeException("Test exception"));

    DataMap result = testGenerator.getAspectData(TEST_DATASET_URN, "testAspect");
    assertNull(result);
  }

  @Test
  public void testGetDownstreamSummaryException() throws Exception {
    when(mockGraphClient.getLineageEntities(any(), any(), anyInt(), anyInt(), anyInt(), any()))
        .thenThrow(new RuntimeException("Test exception"));

    DownstreamSummary summary = testGenerator.getDownstreamSummary(TEST_DATASET_URN);
    assertNull(summary);
  }

  @Test
  public void testBatchGetEntityOwnership() throws Exception {
    Set<Urn> urns =
        ImmutableSet.of(
            UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,test1,PROD)"),
            UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,test2,PROD)"));

    Map<Urn, EntityResponse> responses = new HashMap<>();
    for (Urn urn : urns) {
      Ownership ownership = new Ownership();
      ownership.setOwners(
          new OwnerArray(Collections.singletonList(new Owner().setOwner(TEST_USER_URN))));

      EntityResponse response = new EntityResponse();
      response.setAspects(
          new EnvelopedAspectMap(
              ImmutableMap.of(
                  OWNERSHIP_ASPECT_NAME,
                  new EnvelopedAspect().setValue(new Aspect(ownership.data())))));
      responses.put(urn, response);
    }

    when(mockEntityClient.batchGetV2(
            any(), eq("dataset"), eq(urns), eq(Collections.singleton(OWNERSHIP_ASPECT_NAME))))
        .thenReturn(responses);

    Map<Urn, Ownership> result = testGenerator.batchGetEntityOwnership("dataset", urns);

    assertEquals(result.size(), 2);
    for (Urn urn : urns) {
      assertNotNull(result.get(urn));
      assertEquals(result.get(urn).getOwners().size(), 1);
    }
  }

  @Test
  public void testBatchGetEntityOwnershipEmptySet() {
    Map<Urn, Ownership> result =
        testGenerator.batchGetEntityOwnership("dataset", Collections.emptySet());
    assertTrue(result.isEmpty());
  }

  @Test
  public void testBatchGetEntityOwnershipException() throws Exception {
    Set<Urn> urns = ImmutableSet.of(TEST_DATASET_URN);

    when(mockEntityClient.batchGetV2(any(), any(), any(), any()))
        .thenThrow(new RuntimeException("Test exception"));

    Map<Urn, Ownership> result = testGenerator.batchGetEntityOwnership("dataset", urns);
    assertTrue(result.isEmpty());
  }

  @Test
  public void testGetEntitySubscriptionUrns() throws Exception {
    SearchResult searchResult = new SearchResult();
    searchResult.setEntities(
        new SearchEntityArray(
            Arrays.asList(
                new SearchEntity().setEntity(UrnUtils.getUrn("urn:li:subscription:1")),
                new SearchEntity().setEntity(UrnUtils.getUrn("urn:li:subscription:2")))));

    when(mockEntityClient.filter(
            any(), eq(SUBSCRIPTION_ENTITY_NAME), any(), any(), anyInt(), anyInt()))
        .thenReturn(searchResult);

    Set<Urn> result =
        testGenerator.getEntitySubscriptionUrns(TEST_DATASET_URN, EntityChangeType.OWNER_ADDED);

    assertEquals(result.size(), 2);
  }

  @Test
  public void testGetFilteredSubscriptionUrnsException() throws Exception {
    when(mockEntityClient.filter(any(), any(), any(), any(), anyInt(), anyInt()))
        .thenThrow(new RuntimeException("Test exception"));

    Filter filter = new Filter();
    Set<Urn> result = testGenerator.getFilteredSubscriptionUrns(filter);

    assertTrue(result.isEmpty());
  }

  @Test
  public void testGetDownstreamEntitiesWithMetrics() throws Exception {
    // Setup mock response
    Set<Urn> expectedDownstreams =
        ImmutableSet.of(
            UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,downstream1,PROD)"),
            UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,downstream2,PROD)"));

    EntityLineageResult lineageResult = new EntityLineageResult();
    lineageResult.setRelationships(
        new LineageRelationshipArray(
            expectedDownstreams.stream()
                .map(urn -> new LineageRelationship().setEntity(urn))
                .collect(Collectors.toList())));
    lineageResult.setTotal(2);

    when(mockGraphClient.getLineageEntities(
            eq(TEST_DATASET_URN.toString()),
            eq(LineageDirection.DOWNSTREAM),
            eq(0),
            eq(1000),
            eq(MAX_DOWNSTREAMS_HOP),
            anyString()))
        .thenReturn(lineageResult);

    // Execute
    Set<Urn> result = testGenerator.getDownstreamEntities(TEST_DATASET_URN);

    // Verify
    assertEquals(result, expectedDownstreams);
    verify(mockMetricUtils)
        .increment(eq(testGenerator.getClass()), eq(NOTIFICATIONS_GRAPH_CALL_COUNT), eq(1d));
  }

  @Test
  public void testGetDownstreamSummaryWithMetrics() throws Exception {
    // Setup owners
    Urn downstream1 = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,downstream1,PROD)");
    Urn downstream2 = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,downstream2,PROD)");

    EntityLineageResult lineageResult = new EntityLineageResult();
    lineageResult.setRelationships(
        new LineageRelationshipArray(
            Arrays.asList(
                new LineageRelationship().setEntity(downstream1),
                new LineageRelationship().setEntity(downstream2))));
    lineageResult.setTotal(2);

    when(mockGraphClient.getLineageEntities(
            eq(TEST_DATASET_URN.toString()),
            eq(LineageDirection.DOWNSTREAM),
            eq(0),
            eq(MAX_DOWNSTREAMS_TO_FETCH_OWNERSHIP),
            eq(MAX_DOWNSTREAMS_HOP),
            anyString()))
        .thenReturn(lineageResult);

    // Setup ownership for downstream entities
    Ownership ownership1 = new Ownership();
    ownership1.setOwners(
        new OwnerArray(Collections.singletonList(new Owner().setOwner(TEST_USER_URN))));

    Ownership ownership2 = new Ownership();
    ownership2.setOwners(
        new OwnerArray(Collections.singletonList(new Owner().setOwner(TEST_GROUP_URN))));

    Map<Urn, EntityResponse> ownershipResponses = new HashMap<>();
    EntityResponse response1 = new EntityResponse();
    response1.setAspects(
        new EnvelopedAspectMap(
            ImmutableMap.of(
                OWNERSHIP_ASPECT_NAME,
                new EnvelopedAspect().setValue(new Aspect(ownership1.data())))));
    ownershipResponses.put(downstream1, response1);

    EntityResponse response2 = new EntityResponse();
    response2.setAspects(
        new EnvelopedAspectMap(
            ImmutableMap.of(
                OWNERSHIP_ASPECT_NAME,
                new EnvelopedAspect().setValue(new Aspect(ownership2.data())))));
    ownershipResponses.put(downstream2, response2);

    when(mockEntityClient.batchGetV2(
            any(),
            eq("dataset"),
            eq(ImmutableSet.of(downstream1, downstream2)),
            eq(Collections.singleton(OWNERSHIP_ASPECT_NAME))))
        .thenReturn(ownershipResponses);

    // Execute
    DownstreamSummary summary = testGenerator.getDownstreamSummary(TEST_DATASET_URN);

    // Verify
    assertNotNull(summary);
    assertEquals(summary.getTotal(), 2);
    assertEquals(summary.getAssetUrns().size(), 2);
    assertTrue(summary.getAssetUrns().contains(downstream1));
    assertTrue(summary.getAssetUrns().contains(downstream2));
    assertEquals(summary.getOwnerUrns().size(), 2);
    assertTrue(summary.getOwnerUrns().contains(TEST_USER_URN));
    assertTrue(summary.getOwnerUrns().contains(TEST_GROUP_URN));

    verify(mockMetricUtils)
        .increment(eq(testGenerator.getClass()), eq(NOTIFICATIONS_GRAPH_CALL_COUNT), eq(1d));
  }

  @Test
  public void testGetDownstreamEntitiesNoMetrics() throws Exception {
    // Create operation context without metrics
    operationContext = TestOperationContexts.userContextNoSearchAuthorization(TEST_ACTOR_URN);

    // Recreate test generator with new context
    testGenerator =
        new TestBaseMclNotificationGenerator(
            operationContext,
            mockEventProducer,
            mockEntityClient,
            mockGraphClient,
            mockSettingsService,
            mockRecipientBuilders);

    // Setup mock response
    EntityLineageResult lineageResult = new EntityLineageResult();
    lineageResult.setRelationships(new LineageRelationshipArray());

    when(mockGraphClient.getLineageEntities(any(), any(), anyInt(), anyInt(), anyInt(), any()))
        .thenReturn(lineageResult);

    // Execute
    Set<Urn> result = testGenerator.getDownstreamEntities(TEST_DATASET_URN);

    // Verify no metrics interaction
    assertTrue(result.isEmpty());
    verifyNoInteractions(mockMetricUtils);
  }

  @Test
  public void testGetDownstreamSummaryNoMetrics() throws Exception {
    // Create operation context without metrics
    operationContext = TestOperationContexts.userContextNoSearchAuthorization(TEST_ACTOR_URN);

    // Recreate test generator with new context
    testGenerator =
        new TestBaseMclNotificationGenerator(
            operationContext,
            mockEventProducer,
            mockEntityClient,
            mockGraphClient,
            mockSettingsService,
            mockRecipientBuilders);

    // Setup mock response
    EntityLineageResult lineageResult = new EntityLineageResult();
    lineageResult.setRelationships(new LineageRelationshipArray());
    lineageResult.setTotal(0);

    when(mockGraphClient.getLineageEntities(any(), any(), anyInt(), anyInt(), anyInt(), any()))
        .thenReturn(lineageResult);

    // Execute
    DownstreamSummary summary = testGenerator.getDownstreamSummary(TEST_DATASET_URN);

    // Verify
    assertNotNull(summary);
    assertEquals(summary.getTotal(), 0);
    assertTrue(summary.getAssetUrns().isEmpty());
    assertTrue(summary.getOwnerUrns().isEmpty());
    verifyNoInteractions(mockMetricUtils);
  }
}
