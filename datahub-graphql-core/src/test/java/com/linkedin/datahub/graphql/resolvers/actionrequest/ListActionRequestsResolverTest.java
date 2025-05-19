package com.linkedin.datahub.graphql.resolvers.actionrequest;

import static com.linkedin.datahub.graphql.generated.ActionRequestType.*;
import static com.linkedin.metadata.AcrylConstants.ACTION_REQUEST_STATUS_COMPLETE;
import static com.linkedin.metadata.AcrylConstants.ACTION_REQUEST_STATUS_PENDING;
import static com.linkedin.metadata.Constants.CORP_USER_ENTITY_NAME;
import static com.linkedin.metadata.Constants.GROUP_MEMBERSHIP_ASPECT_NAME;
import static com.linkedin.metadata.Constants.NATIVE_GROUP_MEMBERSHIP_ASPECT_NAME;
import static com.linkedin.metadata.Constants.ROLE_MEMBERSHIP_ASPECT_NAME;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.datahub.authorization.config.ViewAuthorizationConfiguration;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.linkedin.actionrequest.*;
import com.linkedin.actionrequest.ActionRequestParams;
import com.linkedin.actionrequest.ActionRequestStatus;
import com.linkedin.ai.InferenceMetadata;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.*;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.Entity;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.identity.GroupMembership;
import com.linkedin.identity.NativeGroupMembership;
import com.linkedin.identity.RoleMembership;
import com.linkedin.metadata.aspect.ActionRequestAspect;
import com.linkedin.metadata.aspect.ActionRequestAspectArray;
import com.linkedin.metadata.query.filter.*;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.search.SearchResultMetadata;
import com.linkedin.metadata.snapshot.ActionRequestSnapshot;
import com.linkedin.metadata.snapshot.Snapshot;
import com.linkedin.r2.RemoteInvocationException;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.OperationContextConfig;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.net.URISyntaxException;
import java.util.*;
import java.util.stream.Collectors;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/** Basic TestNG test for {@link ListActionRequestsResolver}. */
public class ListActionRequestsResolverTest {
  private static final String TEST_USER_URN = "urn:li:corpuser:testUser";
  private static final String TEST_USER_URN_2 = "urn:li:corpuser:testUser2";
  private static final String DEFAULT_RESOURCE =
      "urn:li:dataset:(urn:li:dataPlatform:hive,myDataset,PROD)";
  private static final String DEFAULT_RESOURCE_2 =
      "urn:li:dataset:(urn:li:dataPlatform:hive,myDataset2,PROD)";
  @Mock private EntityClient mockEntityClient;
  @Mock private DataFetchingEnvironment mockEnvironment;
  @Mock private QueryContext mockContext;

  @Mock
  private OperationContext mockOpContext =
      TestOperationContexts.userContextNoSearchAuthorization(UrnUtils.getUrn(TEST_USER_URN));

  private AutoCloseable mocks;
  private ListActionRequestsResolver resolver;
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

    // Init the User Resolution Calls
    GroupMembership groupMembership = new GroupMembership();
    groupMembership.setGroups(
        new UrnArray(ImmutableList.of(UrnUtils.getUrn("urn:li:corpGroup:group1"))));

    NativeGroupMembership nativeGroupMembership = new NativeGroupMembership();
    nativeGroupMembership.setNativeGroups(
        new UrnArray(ImmutableList.of(UrnUtils.getUrn("urn:li:corpGroup:group2"))));

    RoleMembership roleMembership = new RoleMembership();
    roleMembership.setRoles(new UrnArray(UrnUtils.getUrn("urn:li:dataHubRole:role")));

    EntityResponse userEntityResponse = new EntityResponse();
    userEntityResponse.setAspects(
        new EnvelopedAspectMap(
            ImmutableMap.of(
                GROUP_MEMBERSHIP_ASPECT_NAME,
                new EnvelopedAspect().setValue(new Aspect(groupMembership.data())),
                NATIVE_GROUP_MEMBERSHIP_ASPECT_NAME,
                new EnvelopedAspect().setValue(new Aspect(nativeGroupMembership.data())),
                ROLE_MEMBERSHIP_ASPECT_NAME,
                new EnvelopedAspect().setValue(new Aspect(roleMembership.data())))));

    when(mockEntityClient.getV2(
            any(OperationContext.class),
            eq(CORP_USER_ENTITY_NAME),
            eq(UrnUtils.getUrn(TEST_USER_URN)),
            eq(null)))
        .thenReturn(userEntityResponse);

    initializeTestEntities();
    resolver = new ListActionRequestsResolver(mockEntityClient);
  }

  /** Initialize a collection of test entities that can be reused across tests */
  private void initializeTestEntities() {
    testEntities = new HashMap<>();
    testUrns = new HashMap<>();

    try {
      // Create various test entities with different characteristics

      // Entity 1: TAG_ASSOCIATION, COMPLETE
      createTestEntity(
          "entity1",
          "urn:li:actionRequest:111",
          TAG_ASSOCIATION.toString(),
          ACTION_REQUEST_STATUS_COMPLETE,
          DEFAULT_RESOURCE,
          12345L,
          null);

      // Entity 2: TERM_ASSOCIATION, COMPLETE
      createTestEntity(
          "entity2",
          "urn:li:actionRequest:222",
          TAG_ASSOCIATION.toString(),
          ACTION_REQUEST_STATUS_COMPLETE,
          DEFAULT_RESOURCE,
          23456L,
          null);

      // Entity 3: TAG_ASSOCIATION, PENDING
      createTestEntity(
          "entity3",
          "urn:li:actionRequest:333",
          TAG_ASSOCIATION.toString(),
          ACTION_REQUEST_STATUS_COMPLETE,
          DEFAULT_RESOURCE,
          34567L,
          null);

      // Entity 4: TAG_ASSOCIATION, COMPLETE, different resource
      createTestEntity(
          "entity4",
          "urn:li:actionRequest:444",
          TERM_ASSOCIATION.toString(),
          ACTION_REQUEST_STATUS_COMPLETE,
          DEFAULT_RESOURCE_2,
          45678L,
          null);

      // Entity 5: OWNER_ASSOCIATION, PENDING
      createTestEntity(
          "entity5",
          "urn:li:actionRequest:555",
          DOMAIN_ASSOCIATION.toString(),
          ACTION_REQUEST_STATUS_PENDING,
          DEFAULT_RESOURCE,
          56789L,
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

    }
  }

  @AfterMethod
  public void teardown() throws Exception {
    mocks.close();
  }

  /** Helper method to set up basic environment mocks */
  private void setupEnvironmentMocks(ListActionRequestsInput input) {
    when(mockEnvironment.getArgument("input")).thenReturn(input);
    when(mockEnvironment.getContext()).thenReturn(mockContext);
  }

  @Test
  public void testGetNoAssignee() throws Exception {
    // -----------------------------------------
    // Given: No assignee is set
    // -----------------------------------------
    ListActionRequestsInput input = buildInput(0, 10, null, null, null);
    // Setup environment mocks
    setupEnvironmentMocks(input);

    // Select entities for this test
    List<String> entityKeys = Arrays.asList("entity1");
    // Mock filter output
    SearchResult mockSearchResult =
        new SearchResult()
            .setFrom(0)
            .setPageSize(10)
            .setNumEntities(1)
            .setEntities(
                new SearchEntityArray(
                    entityKeys.stream()
                        .map(key -> testUrns.get(key))
                        .map(urn -> new SearchEntity().setEntity(urn))
                        .collect(Collectors.toList())))
            .setMetadata(new SearchResultMetadata());

    // Mock returns from the EntityClient
    when(mockEntityClient.searchAcrossEntities(
            any(),
            eq(List.of("actionRequest")),
            any(),
            any(Filter.class),
            eq(0),
            eq(10),
            anyList(),
            any(),
            any()))
        .thenReturn(mockSearchResult);

    // Mock batch get response
    mockEntityClientBatchGet(entityKeys);

    // -----------------------------------------
    // When: Invoking the resolver
    // -----------------------------------------
    ListActionRequestsResult result = resolver.get(mockEnvironment).get();

    // -----------------------------------------
    // Then: Validate the result
    // -----------------------------------------
    assertNotNull(result);
    assertEquals(result.getStart(), Integer.valueOf(0));
    assertEquals(result.getCount(), Integer.valueOf(10));
    assertEquals(result.getTotal(), Integer.valueOf(1));
    assertNotNull(result.getActionRequests());
    assertEquals(result.getActionRequests().size(), 1);
    assertEquals(
        result.getActionRequests().get(0).getStatus(),
        com.linkedin.datahub.graphql.generated.ActionRequestStatus.COMPLETED);
    assertEquals(result.getActionRequests().get(0).getAssignedGroups().size(), 0);
    assertEquals(result.getActionRequests().get(0).getAssignedRoles().size(), 0);
    assertEquals(result.getActionRequests().get(0).getAssignedUsers().size(), 0);
    assertEquals(
        result.getActionRequests().get(0).getEntity().getUrn(),
        "urn:li:dataset:(urn:li:dataPlatform:hive,myDataset,PROD)");
    assertEquals(
        result.getActionRequests().get(0).getOrigin(),
        com.linkedin.datahub.graphql.generated.ActionRequestOrigin.MANUAL);
    assertEquals(result.getActionRequests().get(0).getInferenceMetadata().getVersion(), 1);
    assertEquals(
        result.getActionRequests().get(0).getInferenceMetadata().getLastInferredAt(), 98765L);
    assertEquals(
        result.getActionRequests().get(0).getType(),
        com.linkedin.datahub.graphql.generated.ActionRequestType.TAG_ASSOCIATION);
    assertEquals(result.getActionRequests().get(0).getCreated().getActor().getUrn(), TEST_USER_URN);
    assertEquals(
        result.getActionRequests().get(0).getParams().getTagProposal().getTag().getUrn(),
        "urn:li:tag:myTestTag");
  }

  @Test
  public void testGetWithGroupAssignee() throws Exception {
    // -----------------------------------------
    // Given: A group assignee
    // -----------------------------------------
    ActionRequestAssignee assignee = new ActionRequestAssignee();
    assignee.setUrn("urn:li:corpGroup:testGroup");
    assignee.setType(AssigneeType.GROUP);

    ListActionRequestsInput input = buildInput(0, 10, null, null, null);
    // Setup environment mocks
    setupEnvironmentMocks(input);
    // Select entities for this test
    List<String> entityKeys = Arrays.asList("entity2", "entity3");
    // Set up mock search result
    SearchResult mockSearchResult =
        new SearchResult()
            .setFrom(5)
            .setPageSize(5)
            .setNumEntities(2)
            .setEntities(
                new SearchEntityArray(
                    entityKeys.stream()
                        .map(key -> testUrns.get(key))
                        .map(urn -> new SearchEntity().setEntity(urn))
                        .collect(Collectors.toList())))
            .setMetadata(new SearchResultMetadata());

    // Mock returns from the EntityClient
    when(mockEntityClient.searchAcrossEntities(
            any(),
            eq(List.of("actionRequest")),
            any(),
            any(Filter.class),
            eq(0),
            eq(10),
            anyList(),
            any(),
            any()))
        .thenReturn(mockSearchResult);
    // Mock batch get response
    mockEntityClientBatchGet(entityKeys);

    // -----------------------------------------
    // When: We execute the resolver
    // -----------------------------------------
    ListActionRequestsResult result = resolver.get(mockEnvironment).get();

    // -----------------------------------------
    // Then: Validate the result
    // -----------------------------------------
    assertNotNull(result);
    assertEquals(result.getStart(), Integer.valueOf(5));
    assertEquals(result.getCount(), Integer.valueOf(5));
    assertEquals(result.getTotal(), Integer.valueOf(2));
    assertNotNull(result.getActionRequests());
    assertEquals(result.getActionRequests().size(), 2);
    assertEquals(
        result.getActionRequests().get(0).getStatus(),
        com.linkedin.datahub.graphql.generated.ActionRequestStatus.COMPLETED);
    assertEquals(result.getActionRequests().get(0).getAssignedGroups().size(), 0);
    assertEquals(result.getActionRequests().get(0).getAssignedRoles().size(), 0);
    assertEquals(result.getActionRequests().get(0).getAssignedUsers().size(), 0);
    assertEquals(result.getActionRequests().get(0).getEntity().getUrn(), DEFAULT_RESOURCE);
    assertEquals(
        result.getActionRequests().get(0).getOrigin(),
        com.linkedin.datahub.graphql.generated.ActionRequestOrigin.MANUAL);
    assertEquals(result.getActionRequests().get(0).getInferenceMetadata().getVersion(), 1);
    assertEquals(
        result.getActionRequests().get(0).getInferenceMetadata().getLastInferredAt(), 98765L);
    assertEquals(
        result.getActionRequests().get(0).getType(),
        com.linkedin.datahub.graphql.generated.ActionRequestType.TAG_ASSOCIATION);
    assertEquals(result.getActionRequests().get(0).getCreated().getActor().getUrn(), TEST_USER_URN);
    assertEquals(
        result.getActionRequests().get(0).getParams().getTagProposal().getTag().getUrn(),
        "urn:li:tag:myTestTag");
  }

  @Test
  public void testGetListByFilters() throws Exception {
    // -----------------------------------------
    // Given: Input with orFilters
    // -----------------------------------------
    ListActionRequestsInput input = buildInput(0, 10, null, null, null);

    // TODO: Set the orFilters input here

    List<AndFilterInput> orFilters = new ArrayList<>();
    AndFilterInput andFilterInput = new AndFilterInput();
    FacetFilterInput typeFilterInput = new FacetFilterInput();
    typeFilterInput.setField("type");
    typeFilterInput.setValues(ImmutableList.of(DOMAIN_ASSOCIATION.toString()));
    FacetFilterInput createdByFilterInput = new FacetFilterInput();
    createdByFilterInput.setField("createdBy");
    createdByFilterInput.setValues(ImmutableList.of(TEST_USER_URN_2));
    FacetFilterInput statusFilterInput = new FacetFilterInput();
    statusFilterInput.setField("status");
    statusFilterInput.setValues(ImmutableList.of(ACTION_REQUEST_STATUS_PENDING));
    andFilterInput.setAnd(
        ImmutableList.of(typeFilterInput, createdByFilterInput, statusFilterInput));
    orFilters.add(andFilterInput);

    // Set the input orFilters
    input.setOrFilters(orFilters);

    // Setup environment mocks
    setupEnvironmentMocks(input);
    // Select entities for this test
    List<String> entityKeys = Collections.singletonList("entity5");

    // Mock filter output
    SearchResult mockSearchResult =
        new SearchResult()
            .setFrom(0)
            .setPageSize(10)
            .setNumEntities(1)
            .setEntities(
                new SearchEntityArray(
                    entityKeys.stream()
                        .map(key -> testUrns.get(key))
                        .map(urn -> new SearchEntity().setEntity(urn))
                        .collect(Collectors.toList())))
            .setMetadata(new SearchResultMetadata());

    ArgumentCaptor<Filter> filterCaptor = ArgumentCaptor.forClass(Filter.class);
    when(mockEntityClient.searchAcrossEntities(
            any(),
            eq(List.of("actionRequest")),
            any(),
            filterCaptor.capture(),
            eq(0),
            eq(10),
            anyList(),
            any(),
            any()))
        .thenReturn(mockSearchResult);

    // Mock batch get response
    mockEntityClientBatchGet(entityKeys);

    // -----------------------------------------
    // When: Invoking the resolver
    // -----------------------------------------
    ListActionRequestsResult result = resolver.get(mockEnvironment).get();

    // -----------------------------------------
    // Then: Validate the result
    // -----------------------------------------

    // Get the actual filter created by the resolver
    Filter capturedFilter = filterCaptor.getValue();

    System.out.println("CAPTURED_FILTER: " + capturedFilter);
    assertTrue(
        isFilterAppied(capturedFilter, "status", ACTION_REQUEST_STATUS_PENDING),
        "Filter should contain status condition");
    assertTrue(
        isFilterAppied(capturedFilter, "createdBy", TEST_USER_URN_2),
        "Filter should contain createdBy condition");
    assertTrue(
        isFilterAppied(capturedFilter, "type", DOMAIN_ASSOCIATION.toString()),
        "Filter should contain type condition");

    assertNotNull(result);
    assertEquals(result.getStart(), Integer.valueOf(0));
    assertEquals(result.getCount(), Integer.valueOf(10));
    assertEquals(result.getTotal(), Integer.valueOf(1));

    // Verify the returned action request
    assertNotNull(result.getActionRequests());
    assertEquals(result.getActionRequests().size(), 1);
    assertEquals(result.getActionRequests().get(0).getEntity().getUrn(), DEFAULT_RESOURCE);

    System.out.println(result.getActionRequests().get(0).getEntity().getUrn());
    System.out.println(result.getActionRequests().get(0).getType());
    System.out.println(result.getActionRequests().get(0).getStatus());
    System.out.println(result.getActionRequests().get(0).getCreated().getActor().getUrn());

    assertEquals(result.getActionRequests().get(0).getEntity().getUrn(), DEFAULT_RESOURCE);
    assertEquals(result.getActionRequests().get(0).getType(), DOMAIN_ASSOCIATION);
    assertEquals(
        result.getActionRequests().get(0).getStatus().toString(), ACTION_REQUEST_STATUS_PENDING);
    assertEquals(
        result.getActionRequests().get(0).getCreated().getActor().getUrn(), TEST_USER_URN_2);
  }

  @Test
  public void testGetListByResourceUrn() throws Exception {
    // -----------------------------------------
    // Given: Input with resource URN parameter
    // -----------------------------------------
    ListActionRequestsInput input = buildInput(0, 10, DEFAULT_RESOURCE_2, null, null);

    // Setup environment mocks
    setupEnvironmentMocks(input);
    // Select entities for this test
    List<String> entityKeys = Collections.singletonList("entity4");

    // Mock filter output
    SearchResult mockSearchResult =
        new SearchResult()
            .setFrom(0)
            .setPageSize(10)
            .setNumEntities(1)
            .setEntities(
                new SearchEntityArray(
                    entityKeys.stream()
                        .map(key -> testUrns.get(key))
                        .map(urn -> new SearchEntity().setEntity(urn))
                        .collect(Collectors.toList())))
            .setMetadata(new SearchResultMetadata());

    ArgumentCaptor<Filter> filterCaptor = ArgumentCaptor.forClass(Filter.class);
    when(mockEntityClient.searchAcrossEntities(
            any(),
            eq(List.of("actionRequest")),
            any(),
            filterCaptor.capture(),
            eq(0),
            eq(10),
            anyList(),
            any(),
            any()))
        .thenReturn(mockSearchResult);

    // Mock batch get response
    mockEntityClientBatchGet(entityKeys);

    // -----------------------------------------
    // When: Invoking the resolver
    // -----------------------------------------
    ListActionRequestsResult result = resolver.get(mockEnvironment).get();

    // -----------------------------------------
    // Then: Validate the result
    // -----------------------------------------

    // Get the actual filter created by the resolver
    Filter capturedFilter = filterCaptor.getValue();
    assertTrue(
        isFilterAppied(capturedFilter, "resource", DEFAULT_RESOURCE_2),
        "Filter should contain resource URN condition");

    assertNotNull(result);
    assertEquals(result.getStart(), Integer.valueOf(0));
    assertEquals(result.getCount(), Integer.valueOf(10));
    assertEquals(result.getTotal(), Integer.valueOf(1));

    // Verify the returned action request
    assertNotNull(result.getActionRequests());
    assertEquals(result.getActionRequests().size(), 1);
    assertEquals(result.getActionRequests().get(0).getEntity().getUrn(), DEFAULT_RESOURCE_2);
  }

  /** Helper method to build a ListActionRequestsInput with optional assignee fields. */
  private ListActionRequestsInput buildInput(
      Integer start,
      Integer count,
      String resourceUrn,
      ActionRequestAssignee assignee,
      List<AndFilterInput> orFilters) {
    ListActionRequestsInput input = new ListActionRequestsInput();
    input.setStart(start);
    input.setCount(count);
    input.setResourceUrn(resourceUrn);
    input.setAssignee(assignee);
    input.setOrFilters(orFilters);
    input.setFacets(null);
    return input;
  }

  private Boolean isFilterAppied(Filter capturedFilter, String key, String value) {
    boolean foundResourceCondition = false;

    for (ConjunctiveCriterion cc : capturedFilter.getOr()) {
      if (cc.getAnd() != null) {
        for (Criterion c : cc.getAnd()) {
          if (key.equals(c.getField()) && c.getValues() != null && c.getValues().contains(value)) {
            foundResourceCondition = true;
            break;
          }
        }
      }
    }

    return foundResourceCondition;
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

    // Add params based on type. Adding mocks for three types for now..
    ActionRequestParams requestParams = new ActionRequestParams();
    if (type.equals(TAG_ASSOCIATION.toString())) {
      TagProposal tagProposal = new TagProposal();
      tagProposal.setTag(UrnUtils.getUrn("urn:li:tag:myTestTag"));
      tagProposal.setTags(new UrnArray(Collections.emptyList()));
      requestParams.setTagProposal(tagProposal);
    } else if (type.equals(DOMAIN_ASSOCIATION.toString())) {
      DomainProposal domainProposal = new DomainProposal();
      domainProposal.setDomains(new UrnArray());
      requestParams.setDomainProposal(domainProposal);
    } else if (type.equals(CREATE_GLOSSARY_TERM.toString())) {
      CreateGlossaryTermProposal termProposal = new CreateGlossaryTermProposal();
      termProposal.setName("Test Glossary");
      requestParams.setCreateGlossaryTermProposal(termProposal);
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

  @Test
  public void testCreateFilter() throws Exception {
    // Test case 1: With assignee filters (user)
    Urn userUrn = UrnUtils.getUrn(TEST_USER_URN);
    Filter filter1 =
        resolver.createFilter(
            userUrn,
            null,
            null,
            ActionRequestType.TAG_ASSOCIATION,
            com.linkedin.datahub.graphql.generated.ActionRequestStatus.COMPLETED,
            UrnUtils.getUrn(DEFAULT_RESOURCE),
            12345L,
            56789L);
    assertNotNull(filter1);
    assertNotNull(filter1.getOr());
    assertEquals(filter1.getOr().size(), 1);

    assertTrue(isFilterAppied(filter1, "assignedUsers.keyword", TEST_USER_URN));
    assertTrue(isFilterAppied(filter1, "type", TAG_ASSOCIATION.toString()));
    assertTrue(isFilterAppied(filter1, "status", ACTION_REQUEST_STATUS_COMPLETE));
    assertTrue(isFilterAppied(filter1, "resource", DEFAULT_RESOURCE));
    assertTrue(isFilterAppied(filter1, "lastModified", "12345"));
    assertTrue(isFilterAppied(filter1, "lastModified", "56789"));

    // Test case 2: With assignee filters (group)
    Urn groupUrn = UrnUtils.getUrn("urn:li:corpGroup:group1");
    Filter filter2 =
        resolver.createFilter(
            null,
            Collections.singletonList(groupUrn),
            null,
            ActionRequestType.TERM_ASSOCIATION,
            com.linkedin.datahub.graphql.generated.ActionRequestStatus.PENDING,
            null,
            null,
            null);
    assertNotNull(filter2);
    assertNotNull(filter2.getOr());
    assertEquals(filter2.getOr().size(), 1);

    assertTrue(isFilterAppied(filter2, "assignedGroups.keyword", groupUrn.toString()));
    assertTrue(isFilterAppied(filter2, "type", TERM_ASSOCIATION.toString()));
    assertTrue(isFilterAppied(filter2, "status", ACTION_REQUEST_STATUS_PENDING));

    // Test case 3: With non-assignee filters only
    Filter filter3 =
        resolver.createFilter(
            null,
            null,
            null,
            ActionRequestType.DOMAIN_ASSOCIATION,
            com.linkedin.datahub.graphql.generated.ActionRequestStatus.COMPLETED,
            UrnUtils.getUrn(DEFAULT_RESOURCE_2),
            45678L,
            null);
    assertNotNull(filter3);
    assertNotNull(filter3.getOr());
    assertEquals(filter3.getOr().size(), 1);

    assertTrue(isFilterAppied(filter3, "type", DOMAIN_ASSOCIATION.toString()));
    assertTrue(isFilterAppied(filter3, "status", ACTION_REQUEST_STATUS_COMPLETE));
    assertTrue(isFilterAppied(filter3, "resource", DEFAULT_RESOURCE_2));
    assertTrue(isFilterAppied(filter3, "lastModified", "45678"));

    // Test case 4: No filters
    Filter filter4 = resolver.createFilter(null, null, null, null, null, null, null, null);
    assertNotNull(filter4);
    assertNotNull(filter4.getOr());
    assertEquals(filter4.getOr().size(), 0);
  }
}
