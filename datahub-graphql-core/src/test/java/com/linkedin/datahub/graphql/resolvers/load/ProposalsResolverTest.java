package com.linkedin.datahub.graphql.resolvers.load;

import static com.linkedin.datahub.graphql.generated.ActionRequestType.*;
import static com.linkedin.metadata.AcrylConstants.ACTION_REQUEST_STATUS_COMPLETE;
import static com.linkedin.metadata.AcrylConstants.ACTION_REQUEST_STATUS_PENDING;
import static com.linkedin.metadata.Constants.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.datahub.authorization.config.ViewAuthorizationConfiguration;
import com.google.common.collect.ImmutableList;
import com.linkedin.actionrequest.*;
import com.linkedin.actionrequest.ActionRequestParams;
import com.linkedin.actionrequest.ActionRequestStatus;
import com.linkedin.actionrequest.GlossaryTermProposal;
import com.linkedin.ai.InferenceMetadata;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.*;
import com.linkedin.entity.Entity;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.aspect.ActionRequestAspect;
import com.linkedin.metadata.aspect.ActionRequestAspectArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.search.SearchResultMetadata;
import com.linkedin.metadata.service.UserService;
import com.linkedin.metadata.snapshot.ActionRequestSnapshot;
import com.linkedin.metadata.snapshot.Snapshot;
import com.linkedin.r2.RemoteInvocationException;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.OperationContextConfig;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.net.URISyntaxException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/** Basic TestNG test for {@link ProposalsResolver}. */
public class ProposalsResolverTest {
  private static final String TEST_USER_URN = "urn:li:corpuser:testUser";
  private static final String TEST_USER_URN_2 = "urn:li:corpuser:testUser2";
  private static final String TEST_RESOURCE_URN =
      "urn:li:dataset:(urn:li:dataPlatform:hive,myDataset,PROD)";
  private static final String TEST_RESOURCE_URN_2 =
      "urn:li:dataset:(urn:li:dataPlatform:hive,myDataset2,PROD)";

  @Mock private EntityClient mockEntityClient;
  @Mock private UserService mockUserService;
  @Mock private DataFetchingEnvironment mockEnvironment;
  @Mock private QueryContext mockContext;
  @Mock private Function<DataFetchingEnvironment, String> mockUrnProvider;

  @Mock
  private OperationContext mockOpContext =
      TestOperationContexts.userContextNoSearchAuthorization(UrnUtils.getUrn(TEST_USER_URN));

  private AutoCloseable mocks;
  private ProposalsResolver resolver;
  private Map<String, Entity> testEntities;
  private Map<String, Urn> testUrns;

  @BeforeMethod
  public void setup() throws RemoteInvocationException, URISyntaxException {
    mocks = MockitoAnnotations.openMocks(this);
    when(mockContext.getActorUrn()).thenReturn(TEST_USER_URN);
    when(mockContext.getOperationContext()).thenReturn(mockOpContext);
    when(mockOpContext.getOperationContextConfig())
        .thenReturn(
            OperationContextConfig.builder()
                .allowSystemAuthentication(true)
                .viewAuthorizationConfiguration(
                    ViewAuthorizationConfiguration.builder().enabled(false).build())
                .build());

    // Mock URN provider to return test resource URN
    when(mockUrnProvider.apply(any(DataFetchingEnvironment.class))).thenReturn(TEST_RESOURCE_URN);

    // Mock UserService.getUserMemberships call
    UserService.UserMemberships mockUserMemberships =
        new UserService.UserMemberships(
            ImmutableList.of(
                UrnUtils.getUrn("urn:li:corpGroup:group1"),
                UrnUtils.getUrn("urn:li:corpGroup:group2")),
            ImmutableList.of(UrnUtils.getUrn("urn:li:dataHubRole:role")));
    try {
      when(mockUserService.getUserMemberships(any(OperationContext.class), any(Urn.class)))
          .thenReturn(mockUserMemberships);
    } catch (Exception e) {
      throw new RuntimeException("Failed to mock getUserMemberships", e);
    }

    initializeTestEntities();
    resolver = new ProposalsResolver(mockUrnProvider, mockEntityClient, mockUserService);
  }

  /** Initialize a collection of test entities that can be reused across tests */
  private void initializeTestEntities() {
    testEntities = new HashMap<>();
    testUrns = new HashMap<>();

    try {
      // Create various test entities with different characteristics

      // Entity 1: TAG_ASSOCIATION, PENDING, for test resource
      createTestEntity(
          "entity1",
          "urn:li:actionRequest:111",
          TAG_ASSOCIATION.toString(),
          ACTION_REQUEST_STATUS_PENDING,
          TEST_RESOURCE_URN,
          12345L,
          null);

      // Entity 2: TERM_ASSOCIATION, PENDING, for test resource
      createTestEntity(
          "entity2",
          "urn:li:actionRequest:222",
          TERM_ASSOCIATION.toString(),
          ACTION_REQUEST_STATUS_PENDING,
          TEST_RESOURCE_URN,
          23456L,
          null);

      // Entity 3: DOMAIN_ASSOCIATION, COMPLETED, for test resource
      createTestEntity(
          "entity3",
          "urn:li:actionRequest:333",
          DOMAIN_ASSOCIATION.toString(),
          ACTION_REQUEST_STATUS_COMPLETE,
          TEST_RESOURCE_URN,
          34567L,
          null);

      // Entity 4: TAG_ASSOCIATION, PENDING, for different resource
      createTestEntity(
          "entity4",
          "urn:li:actionRequest:444",
          TAG_ASSOCIATION.toString(),
          ACTION_REQUEST_STATUS_PENDING,
          TEST_RESOURCE_URN_2,
          45678L,
          Urn.createFromString(TEST_USER_URN_2));

    } catch (Exception e) {
      throw new RuntimeException("Mocking entities failed", e);
    }
  }

  /** Helper method to create a test entity and add it to the collections */
  private void createTestEntity(
      String key,
      String urnString,
      String type,
      String status,
      String resourceUrn,
      Long createdTimestamp,
      Urn createdBy)
      throws Exception {
    Urn urn = UrnUtils.getUrn(urnString);
    testUrns.put(key, urn);
    testEntities.put(
        key,
        createActionRequestEntity(
            urnString, type, status, resourceUrn, createdTimestamp, createdBy));
  }

  /** Helper to create an entity map from a list of entity keys */
  private Map<Urn, Entity> createEntityMap(List<String> entityKeys) {
    Map<Urn, Entity> entityMap = new HashMap<>();
    for (String key : entityKeys) {
      entityMap.put(testUrns.get(key), testEntities.get(key));
    }
    return entityMap;
  }

  /** Helper to mock entity client to return specific entities */
  private void mockEntityClientBatchGet(List<String> entityKeys) {
    try {
      Set<Urn> urns = entityKeys.stream().map(key -> testUrns.get(key)).collect(Collectors.toSet());

      Map<Urn, Entity> entityMap = createEntityMap(entityKeys);

      when(mockEntityClient.batchGet(any(), eq(urns))).thenReturn(entityMap);
    } catch (Exception e) {
      // Ignore for testing
    }
  }

  @AfterMethod
  public void teardown() throws Exception {
    mocks.close();
  }

  /** Helper method to set up basic environment mocks */
  private void setupEnvironmentMocks() {
    when(mockEnvironment.getContext()).thenReturn(mockContext);
  }

  @Test
  public void testGetPendingProposals() throws Exception {
    // -----------------------------------------
    // Given: Environment with default status (PENDING)
    // -----------------------------------------
    setupEnvironmentMocks();
    when(mockEnvironment.getArgumentOrDefault(
            "status",
            com.linkedin.datahub.graphql.generated.ActionRequestStatus.PENDING.toString()))
        .thenReturn(com.linkedin.datahub.graphql.generated.ActionRequestStatus.PENDING.toString());
    when(mockEnvironment.getArgumentOrDefault("type", null)).thenReturn(null);

    // Select entities for this test (only pending proposals for the test resource)
    List<String> entityKeys = Arrays.asList("entity1", "entity2");

    // Mock filter output
    SearchResult mockSearchResult =
        new SearchResult()
            .setFrom(0)
            .setPageSize(1000)
            .setNumEntities(2)
            .setEntities(
                new SearchEntityArray(
                    entityKeys.stream()
                        .map(key -> testUrns.get(key))
                        .map(urn -> new SearchEntity().setEntity(urn))
                        .collect(Collectors.toList())))
            .setMetadata(new SearchResultMetadata());

    ArgumentCaptor<Filter> filterCaptor = ArgumentCaptor.forClass(Filter.class);
    when(mockEntityClient.filter(
            any(),
            eq(ACTION_REQUEST_ENTITY_NAME),
            filterCaptor.capture(),
            eq(null),
            eq(0),
            eq(1000)))
        .thenReturn(mockSearchResult);

    // Mock batch get response
    mockEntityClientBatchGet(entityKeys);

    // -----------------------------------------
    // When: Invoking the resolver
    // -----------------------------------------
    CompletableFuture<List<ActionRequest>> result = resolver.get(mockEnvironment);
    List<ActionRequest> actionRequests = result.get();

    // -----------------------------------------
    // Then: Validate the result
    // -----------------------------------------
    assertNotNull(actionRequests);
    assertEquals(actionRequests.size(), 2);

    // Verify that user memberships are being used in filter
    Filter capturedFilter = filterCaptor.getValue();
    assertNotNull(capturedFilter);

    // Verify the returned action requests - order independent
    for (ActionRequest request : actionRequests) {
      assertEquals(request.getEntity().getUrn(), TEST_RESOURCE_URN);
      assertEquals(
          request.getStatus(), com.linkedin.datahub.graphql.generated.ActionRequestStatus.PENDING);
      assertTrue(
          request.getType()
                  == com.linkedin.datahub.graphql.generated.ActionRequestType.TAG_ASSOCIATION
              || request.getType()
                  == com.linkedin.datahub.graphql.generated.ActionRequestType.TERM_ASSOCIATION);
    }

    // Verify we have both types
    Set<com.linkedin.datahub.graphql.generated.ActionRequestType> types =
        actionRequests.stream().map(ActionRequest::getType).collect(Collectors.toSet());
    assertTrue(
        types.contains(com.linkedin.datahub.graphql.generated.ActionRequestType.TAG_ASSOCIATION));
    assertTrue(
        types.contains(com.linkedin.datahub.graphql.generated.ActionRequestType.TERM_ASSOCIATION));
  }

  @Test
  public void testGetCompletedProposals() throws Exception {
    // -----------------------------------------
    // Given: Environment requesting completed proposals
    // -----------------------------------------
    setupEnvironmentMocks();
    when(mockEnvironment.getArgumentOrDefault(
            "status",
            com.linkedin.datahub.graphql.generated.ActionRequestStatus.PENDING.toString()))
        .thenReturn(
            com.linkedin.datahub.graphql.generated.ActionRequestStatus.COMPLETED.toString());
    when(mockEnvironment.getArgumentOrDefault("type", null)).thenReturn(null);

    // Select entities for this test (only completed proposals for the test resource)
    List<String> entityKeys = Arrays.asList("entity3");

    // Mock filter output
    SearchResult mockSearchResult =
        new SearchResult()
            .setFrom(0)
            .setPageSize(1000)
            .setNumEntities(1)
            .setEntities(
                new SearchEntityArray(
                    entityKeys.stream()
                        .map(key -> testUrns.get(key))
                        .map(urn -> new SearchEntity().setEntity(urn))
                        .collect(Collectors.toList())))
            .setMetadata(new SearchResultMetadata());

    when(mockEntityClient.filter(
            any(), eq(ACTION_REQUEST_ENTITY_NAME), any(Filter.class), eq(null), eq(0), eq(1000)))
        .thenReturn(mockSearchResult);

    // Mock batch get response
    mockEntityClientBatchGet(entityKeys);

    // -----------------------------------------
    // When: Invoking the resolver
    // -----------------------------------------
    CompletableFuture<List<ActionRequest>> result = resolver.get(mockEnvironment);
    List<ActionRequest> actionRequests = result.get();

    // -----------------------------------------
    // Then: Validate the result
    // -----------------------------------------
    assertNotNull(actionRequests);
    assertEquals(actionRequests.size(), 1);

    assertEquals(actionRequests.get(0).getEntity().getUrn(), TEST_RESOURCE_URN);
    assertEquals(
        actionRequests.get(0).getStatus(),
        com.linkedin.datahub.graphql.generated.ActionRequestStatus.COMPLETED);
    assertEquals(
        actionRequests.get(0).getType(),
        com.linkedin.datahub.graphql.generated.ActionRequestType.DOMAIN_ASSOCIATION);
  }

  @Test
  public void testGetProposalsByType() throws Exception {
    // -----------------------------------------
    // Given: Environment requesting proposals of specific type
    // -----------------------------------------
    setupEnvironmentMocks();
    when(mockEnvironment.getArgumentOrDefault(
            "status",
            com.linkedin.datahub.graphql.generated.ActionRequestStatus.PENDING.toString()))
        .thenReturn(com.linkedin.datahub.graphql.generated.ActionRequestStatus.PENDING.toString());
    when(mockEnvironment.getArgumentOrDefault("type", null)).thenReturn(TAG_ASSOCIATION.toString());

    // Select entities for this test (only TAG_ASSOCIATION proposals)
    List<String> entityKeys = Arrays.asList("entity1");

    // Mock filter output
    SearchResult mockSearchResult =
        new SearchResult()
            .setFrom(0)
            .setPageSize(1000)
            .setNumEntities(1)
            .setEntities(
                new SearchEntityArray(
                    entityKeys.stream()
                        .map(key -> testUrns.get(key))
                        .map(urn -> new SearchEntity().setEntity(urn))
                        .collect(Collectors.toList())))
            .setMetadata(new SearchResultMetadata());

    when(mockEntityClient.filter(
            any(), eq(ACTION_REQUEST_ENTITY_NAME), any(Filter.class), eq(null), eq(0), eq(1000)))
        .thenReturn(mockSearchResult);

    // Mock batch get response
    mockEntityClientBatchGet(entityKeys);

    // -----------------------------------------
    // When: Invoking the resolver
    // -----------------------------------------
    CompletableFuture<List<ActionRequest>> result = resolver.get(mockEnvironment);
    List<ActionRequest> actionRequests = result.get();

    // -----------------------------------------
    // Then: Validate the result
    // -----------------------------------------
    assertNotNull(actionRequests);
    assertEquals(actionRequests.size(), 1);
    assertEquals(
        actionRequests.get(0).getType(),
        com.linkedin.datahub.graphql.generated.ActionRequestType.TAG_ASSOCIATION);
    assertEquals(
        actionRequests.get(0).getStatus(),
        com.linkedin.datahub.graphql.generated.ActionRequestStatus.PENDING);
  }

  @Test
  public void testUserServiceIntegration() throws Exception {
    // -----------------------------------------
    // Given: Mock specific user memberships
    // -----------------------------------------
    setupEnvironmentMocks();
    when(mockEnvironment.getArgumentOrDefault(
            "status",
            com.linkedin.datahub.graphql.generated.ActionRequestStatus.PENDING.toString()))
        .thenReturn(com.linkedin.datahub.graphql.generated.ActionRequestStatus.PENDING.toString());
    when(mockEnvironment.getArgumentOrDefault("type", null)).thenReturn(null);

    // Reset the UserService mock to return specific memberships
    UserService.UserMemberships specificMemberships =
        new UserService.UserMemberships(
            ImmutableList.of(UrnUtils.getUrn("urn:li:corpGroup:specificGroup")),
            ImmutableList.of(UrnUtils.getUrn("urn:li:dataHubRole:specificRole")));
    try {
      when(mockUserService.getUserMemberships(
              any(OperationContext.class), eq(UrnUtils.getUrn(TEST_USER_URN))))
          .thenReturn(specificMemberships);
    } catch (Exception e) {
      throw new RuntimeException("Failed to mock getUserMemberships", e);
    }

    List<String> entityKeys = Arrays.asList("entity1");
    SearchResult mockSearchResult =
        new SearchResult()
            .setFrom(0)
            .setPageSize(1000)
            .setNumEntities(1)
            .setEntities(
                new SearchEntityArray(
                    entityKeys.stream()
                        .map(key -> testUrns.get(key))
                        .map(urn -> new SearchEntity().setEntity(urn))
                        .collect(Collectors.toList())))
            .setMetadata(new SearchResultMetadata());

    when(mockEntityClient.filter(
            any(), eq(ACTION_REQUEST_ENTITY_NAME), any(Filter.class), eq(null), eq(0), eq(1000)))
        .thenReturn(mockSearchResult);

    mockEntityClientBatchGet(entityKeys);

    // -----------------------------------------
    // When: Invoking the resolver
    // -----------------------------------------
    CompletableFuture<List<ActionRequest>> result = resolver.get(mockEnvironment);
    result.get();

    // -----------------------------------------
    // Then: Verify UserService was called with correct parameters
    // -----------------------------------------
    verify(mockUserService)
        .getUserMemberships(eq(mockOpContext), eq(UrnUtils.getUrn(TEST_USER_URN)));
  }

  /** Helper to create action request entity with given parameters. */
  private Entity createActionRequestEntity(
      String urn,
      String type,
      String status,
      String resourceUrn,
      Long createdTimestamp,
      Urn createdBy)
      throws Exception {
    ActionRequestInfo infoAspect = new ActionRequestInfo();
    infoAspect.setType(type);
    infoAspect.setResource(resourceUrn);
    infoAspect.setCreated(createdTimestamp != null ? createdTimestamp : 12345L);
    infoAspect.setCreatedBy(createdBy != null ? createdBy : Urn.createFromString(TEST_USER_URN));
    infoAspect.setOrigin(com.linkedin.actionrequest.ActionRequestOrigin.MANUAL);
    infoAspect.setAssignedUsers(new UrnArray());
    infoAspect.setAssignedGroups(new UrnArray());
    infoAspect.setAssignedRoles(new UrnArray());

    // Add params based on type
    ActionRequestParams requestParams = new ActionRequestParams();
    if (type.equals(TAG_ASSOCIATION.toString())) {
      TagProposal tagProposal = new TagProposal();
      tagProposal.setTag(UrnUtils.getUrn("urn:li:tag:myTestTag"));
      tagProposal.setTags(new UrnArray(Collections.emptyList()));
      requestParams.setTagProposal(tagProposal);
    } else if (type.equals(TERM_ASSOCIATION.toString())) {
      GlossaryTermProposal termProposal = new GlossaryTermProposal();
      termProposal.setGlossaryTerms(new UrnArray(Collections.emptyList()));
      requestParams.setGlossaryTermProposal(termProposal);
    } else if (type.equals(DOMAIN_ASSOCIATION.toString())) {
      DomainProposal domainProposal = new DomainProposal();
      domainProposal.setDomains(new UrnArray());
      requestParams.setDomainProposal(domainProposal);
    }

    infoAspect.setParams(requestParams);

    // Add inference metadata
    infoAspect.setInferenceMetadata(
        new InferenceMetadata().setVersion(1).setLastInferredAt(98765L));

    // Create status aspect
    ActionRequestStatus statusAspect = new ActionRequestStatus();
    statusAspect.setStatus(status);

    // Create aspects
    ActionRequestAspect aspect1 = ActionRequestAspect.create(infoAspect);
    ActionRequestAspect aspect2 = ActionRequestAspect.create(statusAspect);

    // Create entity
    Entity entity = new Entity();
    ActionRequestSnapshot snapshot = new ActionRequestSnapshot();
    snapshot.setUrn(UrnUtils.getUrn(urn));
    snapshot.setAspects(new ActionRequestAspectArray(Arrays.asList(aspect1, aspect2)));
    entity.setValue(Snapshot.create(snapshot));

    return entity;
  }
}
