package com.linkedin.datahub.graphql.resolvers.actionrequest;

import static com.linkedin.datahub.graphql.generated.ActionRequestType.TAG_ASSOCIATION;
import static com.linkedin.metadata.AcrylConstants.ACTION_REQUEST_STATUS_COMPLETE;
import static com.linkedin.metadata.Constants.CORP_USER_ENTITY_NAME;
import static com.linkedin.metadata.Constants.GROUP_MEMBERSHIP_ASPECT_NAME;
import static com.linkedin.metadata.Constants.NATIVE_GROUP_MEMBERSHIP_ASPECT_NAME;
import static com.linkedin.metadata.Constants.ROLE_MEMBERSHIP_ASPECT_NAME;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.datahub.authorization.config.ViewAuthorizationConfiguration;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.linkedin.actionrequest.ActionRequestInfo;
import com.linkedin.actionrequest.ActionRequestParams;
import com.linkedin.actionrequest.ActionRequestStatus;
import com.linkedin.actionrequest.TagProposal;
import com.linkedin.ai.InferenceMetadata;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.ActionRequestAssignee;
import com.linkedin.datahub.graphql.generated.AssigneeType;
import com.linkedin.datahub.graphql.generated.ListActionRequestsInput;
import com.linkedin.datahub.graphql.generated.ListActionRequestsResult;
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
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.snapshot.ActionRequestSnapshot;
import com.linkedin.metadata.snapshot.Snapshot;
import com.linkedin.r2.RemoteInvocationException;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.OperationContextConfig;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Basic TestNG test for {@link ListActionRequestsResolver}.
 *
 * <p>TODO: We need better tests for the specific filter criteria being built. This test is quite
 * basic checks.
 */
public class ListActionRequestsResolverTest {
  private static final String TEST_USER_URN = "urn:li:corpuser:testUser";
  @Mock private EntityClient mockEntityClient;
  @Mock private DataFetchingEnvironment mockEnvironment;
  @Mock private QueryContext mockContext;

  @Mock
  private OperationContext mockOpContext =
      TestOperationContexts.userContextNoSearchAuthorization(UrnUtils.getUrn(TEST_USER_URN));

  private AutoCloseable mocks;
  private ListActionRequestsResolver resolver;

  @BeforeMethod
  public void setup() throws RemoteInvocationException, URISyntaxException {
    mocks = MockitoAnnotations.openMocks(this);
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

    resolver = new ListActionRequestsResolver(mockEntityClient);
  }

  @AfterMethod
  public void teardown() throws Exception {
    mocks.close();
  }

  @Test
  public void testGetNoAssignee() throws Exception {
    // -----------------------------------------
    // Given: No assignee is set
    // -----------------------------------------
    ListActionRequestsInput input = buildInput(0, 10, null, null);

    // Environment & context mocks
    when(mockEnvironment.getArgument("input")).thenReturn(input);
    when(mockEnvironment.getContext()).thenReturn(mockContext);
    // The current user
    when(mockContext.getActorUrn()).thenReturn(TEST_USER_URN);

    // Mock filter output
    SearchResult mockSearchResult =
        new SearchResult()
            .setFrom(0)
            .setPageSize(10)
            .setNumEntities(1)
            .setEntities(
                new SearchEntityArray(
                    Collections.singletonList(
                        new SearchEntity()
                            .setEntity(UrnUtils.getUrn("urn:li:actionRequest:111")))));

    // Mock returns from the EntityClient
    when(mockEntityClient.filter(
            any(), eq("actionRequest"), any(Filter.class), anyList(), eq(0), eq(10)))
        .thenReturn(mockSearchResult);

    TagProposal tagProposal = new TagProposal();
    tagProposal.setTag(UrnUtils.getUrn("urn:li:tag:myTestTag"));
    tagProposal.setTags(new UrnArray(Collections.emptyList()));

    ActionRequestParams requestParams = new ActionRequestParams();
    requestParams.setTagProposal(tagProposal);

    ActionRequestInfo infoAspect = new ActionRequestInfo();
    infoAspect.setType(TAG_ASSOCIATION.toString());
    infoAspect.setResource("urn:li:dataset:(urn:li:dataPlatform:hive,myDataset,PROD)");
    infoAspect.setCreated(12345L);
    infoAspect.setCreatedBy(Urn.createFromString(TEST_USER_URN));
    infoAspect.setParams(requestParams);
    infoAspect.setOrigin(com.linkedin.actionrequest.ActionRequestOrigin.MANUAL);
    infoAspect.setInferenceMetadata(
        new InferenceMetadata().setVersion(1).setLastInferredAt(98765L));
    infoAspect.setAssignedUsers(new UrnArray());
    infoAspect.setAssignedGroups(new UrnArray());
    infoAspect.setAssignedRoles(new UrnArray());

    ActionRequestStatus status = new ActionRequestStatus();
    status.setStatus(ACTION_REQUEST_STATUS_COMPLETE);

    ActionRequestAspect aspect1 = ActionRequestAspect.create(infoAspect);
    ActionRequestAspect aspect2 = ActionRequestAspect.create(status);

    Entity mockEntity = new Entity();
    ActionRequestSnapshot mockSnapshot = new ActionRequestSnapshot();
    mockSnapshot.setUrn(UrnUtils.getUrn("urn:li:actionRequest:111"));
    mockSnapshot.setAspects(new ActionRequestAspectArray(Arrays.asList(aspect1, aspect2)));
    mockEntity.setValue(Snapshot.create(mockSnapshot));

    Map<Urn, Entity> entityMap = new HashMap<>();
    entityMap.put(UrnUtils.getUrn("urn:li:actionRequest:111"), mockEntity);

    when(mockEntityClient.batchGet(
            any(),
            eq(
                new HashSet<>(
                    Collections.singletonList(UrnUtils.getUrn("urn:li:actionRequest:111"))))))
        .thenReturn(entityMap);

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

    ListActionRequestsInput input = buildInput(0, 10, null, assignee);

    // Mock environment & context
    when(mockEnvironment.getArgument("input")).thenReturn(input);
    when(mockEnvironment.getContext()).thenReturn(mockContext);
    // The current user is not used for filters if an assignee is provided
    when(mockContext.getActorUrn()).thenReturn(TEST_USER_URN);

    // Set up mock search result
    SearchResult mockSearchResult =
        new SearchResult()
            .setFrom(5)
            .setPageSize(5)
            .setNumEntities(2)
            .setEntities(
                new SearchEntityArray(
                    Arrays.asList(
                        new SearchEntity().setEntity(UrnUtils.getUrn("urn:li:actionRequest:222")),
                        new SearchEntity()
                            .setEntity(UrnUtils.getUrn("urn:li:actionRequest:333")))));

    when(mockEntityClient.filter(
            any(), eq("actionRequest"), any(Filter.class), anyList(), eq(0), eq(10)))
        .thenReturn(mockSearchResult);

    TagProposal tagProposal = new TagProposal();
    tagProposal.setTag(UrnUtils.getUrn("urn:li:tag:myTestTag"));
    tagProposal.setTags(new UrnArray(Collections.emptyList()));

    ActionRequestParams requestParams = new ActionRequestParams();
    requestParams.setTagProposal(tagProposal);

    ActionRequestInfo infoAspect = new ActionRequestInfo();
    infoAspect.setType(TAG_ASSOCIATION.toString());
    infoAspect.setResource("urn:li:dataset:(urn:li:dataPlatform:hive,myDataset,PROD)");
    infoAspect.setCreated(12345L);
    infoAspect.setCreatedBy(Urn.createFromString(TEST_USER_URN));
    infoAspect.setParams(requestParams);
    infoAspect.setOrigin(com.linkedin.actionrequest.ActionRequestOrigin.MANUAL);
    infoAspect.setInferenceMetadata(
        new InferenceMetadata().setVersion(1).setLastInferredAt(98765L));
    infoAspect.setAssignedUsers(new UrnArray());
    infoAspect.setAssignedGroups(new UrnArray());
    infoAspect.setAssignedRoles(new UrnArray());

    ActionRequestStatus status = new ActionRequestStatus();
    status.setStatus(ACTION_REQUEST_STATUS_COMPLETE);

    ActionRequestAspect aspect1 = ActionRequestAspect.create(infoAspect);
    ActionRequestAspect aspect2 = ActionRequestAspect.create(status);

    Entity mockEntity1 = new Entity();
    ActionRequestSnapshot mockSnapshot1 = new ActionRequestSnapshot();
    mockSnapshot1.setUrn(UrnUtils.getUrn("urn:li:actionRequest:222"));
    mockSnapshot1.setAspects(new ActionRequestAspectArray(Arrays.asList(aspect1, aspect2)));
    mockEntity1.setValue(Snapshot.create(mockSnapshot1));

    Entity mockEntity2 = new Entity();
    ActionRequestSnapshot mockSnapshot2 = new ActionRequestSnapshot();
    mockSnapshot2.setUrn(UrnUtils.getUrn("urn:li:actionRequest:333"));
    mockSnapshot2.setAspects(new ActionRequestAspectArray(Arrays.asList(aspect1, aspect2)));
    mockEntity2.setValue(Snapshot.create(mockSnapshot2));

    Map<Urn, Entity> entityMap = new HashMap<>();
    entityMap.put(UrnUtils.getUrn("urn:li:actionRequest:222"), mockEntity1);
    entityMap.put(UrnUtils.getUrn("urn:li:actionRequest:333"), mockEntity2);

    when(mockEntityClient.batchGet(
            any(),
            eq(
                new HashSet<>(
                    Arrays.asList(
                        UrnUtils.getUrn("urn:li:actionRequest:222"),
                        UrnUtils.getUrn("urn:li:actionRequest:333"))))))
        .thenReturn(entityMap);

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

  /** Helper method to build a ListActionRequestsInput with optional assignee fields. */
  private ListActionRequestsInput buildInput(
      Integer start, Integer count, String resourceUrn, ActionRequestAssignee assignee) {
    ListActionRequestsInput input = new ListActionRequestsInput();
    input.setStart(start);
    input.setCount(count);
    input.setResourceUrn(resourceUrn);
    input.setAssignee(assignee);
    return input;
  }
}
