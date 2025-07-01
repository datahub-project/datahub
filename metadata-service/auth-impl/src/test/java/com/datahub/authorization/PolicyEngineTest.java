package com.datahub.authorization;

import static com.linkedin.metadata.Constants.*;
import static com.linkedin.metadata.authorization.PoliciesConfig.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.Owner;
import com.linkedin.common.OwnerArray;
import com.linkedin.common.Ownership;
import com.linkedin.common.OwnershipType;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.StringArray;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.identity.RoleMembership;
import com.linkedin.metadata.Constants;
import com.linkedin.policy.*;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.net.URISyntaxException;
import java.util.*;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class PolicyEngineTest {

  private static final String AUTHORIZED_PRINCIPAL = "urn:li:corpuser:datahub";
  private static final String UNAUTHORIZED_PRINCIPAL = "urn:li:corpuser:unauthorized";
  private static final String AUTHORIZED_GROUP = "urn:li:corpGroup:authorizedGroup";
  private static final String RESOURCE_URN = "urn:li:dataset:test";
  private static final String DOMAIN_URN = "urn:li:domain:domain1";
  private static final String CONTAINER_URN = "urn:li:container:container1";
  private static final String TAG_URN = "urn:li:tag:allowed";
  private static final String OWNERSHIP_TYPE_URN = "urn:li:ownershipType:__system__technical_owner";
  private static final String OTHER_OWNERSHIP_TYPE_URN =
      "urn:li:ownershipType:__system__data_steward";

  private EntityClient _entityClient;
  private OperationContext systemOperationContext;
  private PolicyEngine _policyEngine;

  private Urn authorizedUserUrn;
  private ResolvedEntitySpec resolvedAuthorizedUserSpec;
  private Urn unauthorizedUserUrn;
  private ResolvedEntitySpec resolvedUnauthorizedUserSpec;
  private Urn resourceUrn;

  @BeforeMethod
  public void setupTest() throws Exception {
    _entityClient = Mockito.mock(EntityClient.class);
    systemOperationContext = TestOperationContexts.systemContextNoSearchAuthorization();
    _policyEngine = new PolicyEngine(_entityClient);

    authorizedUserUrn = Urn.createFromString(AUTHORIZED_PRINCIPAL);
    resolvedAuthorizedUserSpec =
        buildEntityResolvers(
            CORP_USER_ENTITY_NAME,
            AUTHORIZED_PRINCIPAL,
            Collections.emptySet(),
            Collections.emptySet(),
            Collections.emptySet(),
            Collections.singleton(AUTHORIZED_GROUP),
            Collections.emptySet());
    unauthorizedUserUrn = Urn.createFromString(UNAUTHORIZED_PRINCIPAL);
    resolvedUnauthorizedUserSpec =
        buildEntityResolvers(CORP_USER_ENTITY_NAME, UNAUTHORIZED_PRINCIPAL);
    resourceUrn = Urn.createFromString(RESOURCE_URN);

    // Init role membership mocks.
    EntityResponse authorizedEntityResponse = createAuthorizedEntityResponse();
    authorizedEntityResponse.setUrn(authorizedUserUrn);
    Map<Urn, EntityResponse> authorizedEntityResponseMap =
        Collections.singletonMap(authorizedUserUrn, authorizedEntityResponse);
    when(_entityClient.batchGetV2(
            eq(systemOperationContext),
            eq(CORP_USER_ENTITY_NAME),
            eq(Collections.singleton(authorizedUserUrn)),
            eq(
                ImmutableSet.of(
                    ROLE_MEMBERSHIP_ASPECT_NAME,
                    GROUP_MEMBERSHIP_ASPECT_NAME,
                    NATIVE_GROUP_MEMBERSHIP_ASPECT_NAME))))
        .thenReturn(authorizedEntityResponseMap);

    EntityResponse unauthorizedEntityResponse = createUnauthorizedEntityResponse();
    unauthorizedEntityResponse.setUrn(unauthorizedUserUrn);
    Map<Urn, EntityResponse> unauthorizedEntityResponseMap =
        Collections.singletonMap(unauthorizedUserUrn, unauthorizedEntityResponse);
    when(_entityClient.batchGetV2(
            eq(systemOperationContext),
            eq(CORP_USER_ENTITY_NAME),
            eq(Collections.singleton(unauthorizedUserUrn)),
            eq(
                ImmutableSet.of(
                    ROLE_MEMBERSHIP_ASPECT_NAME,
                    GROUP_MEMBERSHIP_ASPECT_NAME,
                    NATIVE_GROUP_MEMBERSHIP_ASPECT_NAME))))
        .thenReturn(unauthorizedEntityResponseMap);

    // Init ownership type mocks.
    EntityResponse entityResponse = new EntityResponse();
    EnvelopedAspectMap envelopedAspectMap = new EnvelopedAspectMap();
    envelopedAspectMap.put(
        OWNERSHIP_ASPECT_NAME,
        new EnvelopedAspect()
            .setValue(new com.linkedin.entity.Aspect(createOwnershipAspect(true, true).data())));
    entityResponse.setAspects(envelopedAspectMap);
    Map<Urn, EntityResponse> mockMap = mock(Map.class);
    when(_entityClient.batchGetV2(
            eq(systemOperationContext),
            any(),
            eq(Collections.singleton(resourceUrn)),
            eq(Collections.singleton(OWNERSHIP_ASPECT_NAME))))
        .thenReturn(mockMap);
    when(mockMap.get(eq(resourceUrn))).thenReturn(entityResponse);
  }

  @Test
  public void testEvaluatePolicyInactivePolicyState() {

    final DataHubPolicyInfo dataHubPolicyInfo = new DataHubPolicyInfo();
    dataHubPolicyInfo.setType(METADATA_POLICY_TYPE);
    dataHubPolicyInfo.setState(INACTIVE_POLICY_STATE);
    dataHubPolicyInfo.setPrivileges(new StringArray("EDIT_ENTITY_TAGS"));
    dataHubPolicyInfo.setDisplayName("My Test Display");
    dataHubPolicyInfo.setDescription("My test display!");
    dataHubPolicyInfo.setEditable(true);

    final DataHubActorFilter actorFilter = new DataHubActorFilter();
    actorFilter.setGroups(new UrnArray());
    actorFilter.setUsers(new UrnArray());
    actorFilter.setResourceOwners(false);
    actorFilter.setAllUsers(true);
    actorFilter.setAllGroups(true);
    dataHubPolicyInfo.setActors(actorFilter);

    final DataHubResourceFilter resourceFilter = new DataHubResourceFilter();
    resourceFilter.setAllResources(true);
    resourceFilter.setType("dataset");
    dataHubPolicyInfo.setResources(resourceFilter);
    ResolvedEntitySpec resourceSpec = buildEntityResolvers("dataset", RESOURCE_URN);
    PolicyEngine.PolicyEvaluationResult result =
        _policyEngine.evaluatePolicy(
            systemOperationContext,
            dataHubPolicyInfo,
            resolvedAuthorizedUserSpec,
            "EDIT_ENTITY_TAGS",
            Optional.of(resourceSpec),
            Collections.emptyList());

    assertFalse(result.isGranted());
  }

  @Test
  public void testEvaluatePolicyPrivilegeFilterNoMatch() throws Exception {
    final DataHubPolicyInfo dataHubPolicyInfo = new DataHubPolicyInfo();
    dataHubPolicyInfo.setType(METADATA_POLICY_TYPE);
    dataHubPolicyInfo.setState(ACTIVE_POLICY_STATE);
    dataHubPolicyInfo.setPrivileges(new StringArray("EDIT_ENTITY_TAGS"));
    dataHubPolicyInfo.setDisplayName("My Test Display");
    dataHubPolicyInfo.setDescription("My test display!");
    dataHubPolicyInfo.setEditable(true);

    final DataHubActorFilter actorFilter = new DataHubActorFilter();
    actorFilter.setResourceOwners(true);
    actorFilter.setAllUsers(false);
    actorFilter.setAllGroups(false);
    dataHubPolicyInfo.setActors(actorFilter);

    final DataHubResourceFilter resourceFilter = new DataHubResourceFilter();
    resourceFilter.setAllResources(true);
    resourceFilter.setType("dataset");
    dataHubPolicyInfo.setResources(resourceFilter);

    ResolvedEntitySpec resourceSpec = buildEntityResolvers("dataset", RESOURCE_URN);
    PolicyEngine.PolicyEvaluationResult result =
        _policyEngine.evaluatePolicy(
            systemOperationContext,
            dataHubPolicyInfo,
            resolvedAuthorizedUserSpec,
            "EDIT_ENTITY_OWNERS",
            Optional.of(resourceSpec),
            Collections.emptyList());
    assertFalse(result.isGranted());

    // Verify no network calls
    verify(_entityClient, times(0)).batchGetV2(any(), any(), any(), any());
  }

  @Test
  public void testEvaluatePlatformPolicyPrivilegeFilterMatch() throws Exception {
    final DataHubPolicyInfo dataHubPolicyInfo = new DataHubPolicyInfo();
    dataHubPolicyInfo.setType(PLATFORM_POLICY_TYPE);
    dataHubPolicyInfo.setState(ACTIVE_POLICY_STATE);
    dataHubPolicyInfo.setPrivileges(new StringArray("MANAGE_POLICIES"));
    dataHubPolicyInfo.setDisplayName("My Test Display");
    dataHubPolicyInfo.setDescription("My test display!");
    dataHubPolicyInfo.setEditable(true);

    final DataHubActorFilter actorFilter = new DataHubActorFilter();
    actorFilter.setResourceOwners(false);
    actorFilter.setAllUsers(true);
    actorFilter.setAllGroups(false);
    dataHubPolicyInfo.setActors(actorFilter);

    PolicyEngine.PolicyEvaluationResult result =
        _policyEngine.evaluatePolicy(
            systemOperationContext,
            dataHubPolicyInfo,
            resolvedAuthorizedUserSpec,
            "MANAGE_POLICIES",
            Optional.empty(),
            Collections.emptyList());
    assertTrue(result.isGranted());

    // Verify no network calls
    verify(_entityClient, times(0)).batchGetV2(any(), any(), any(), any());
  }

  @Test
  public void testEvaluatePolicyActorFilterUserMatch() throws Exception {

    final DataHubPolicyInfo dataHubPolicyInfo = new DataHubPolicyInfo();
    dataHubPolicyInfo.setType(METADATA_POLICY_TYPE);
    dataHubPolicyInfo.setState(ACTIVE_POLICY_STATE);
    dataHubPolicyInfo.setPrivileges(new StringArray("EDIT_ENTITY_TAGS"));
    dataHubPolicyInfo.setDisplayName("My Test Display");
    dataHubPolicyInfo.setDescription("My test display!");
    dataHubPolicyInfo.setEditable(true);

    final DataHubActorFilter actorFilter = new DataHubActorFilter();
    final UrnArray usersUrnArray = new UrnArray();
    usersUrnArray.add(Urn.createFromString(AUTHORIZED_PRINCIPAL));
    actorFilter.setUsers(usersUrnArray);
    actorFilter.setResourceOwners(false);
    actorFilter.setAllUsers(false);
    actorFilter.setAllGroups(false);
    dataHubPolicyInfo.setActors(actorFilter);

    final DataHubResourceFilter resourceFilter = new DataHubResourceFilter();
    resourceFilter.setAllResources(true);
    resourceFilter.setType("dataset");
    dataHubPolicyInfo.setResources(resourceFilter);

    ResolvedEntitySpec resourceSpec = buildEntityResolvers("dataset", RESOURCE_URN);
    // Assert Authorized user can edit entity tags.
    PolicyEngine.PolicyEvaluationResult result1 =
        _policyEngine.evaluatePolicy(
            systemOperationContext,
            dataHubPolicyInfo,
            resolvedAuthorizedUserSpec,
            "EDIT_ENTITY_TAGS",
            Optional.of(resourceSpec),
            Collections.emptyList());

    assertTrue(result1.isGranted());

    // Verify we are not making any network calls for these predicates.
    verify(_entityClient, times(0)).batchGetV2(any(), any(), any(), any());
  }

  @Test
  public void testEvaluatePolicyActorFilterUserNoMatch() throws Exception {

    final DataHubPolicyInfo dataHubPolicyInfo = new DataHubPolicyInfo();
    dataHubPolicyInfo.setType(METADATA_POLICY_TYPE);
    dataHubPolicyInfo.setState(ACTIVE_POLICY_STATE);
    dataHubPolicyInfo.setPrivileges(new StringArray("EDIT_ENTITY_TAGS"));
    dataHubPolicyInfo.setDisplayName("My Test Display");
    dataHubPolicyInfo.setDescription("My test display!");
    dataHubPolicyInfo.setEditable(true);

    final DataHubActorFilter actorFilter = new DataHubActorFilter();
    final UrnArray usersUrnArray = new UrnArray();
    usersUrnArray.add(Urn.createFromString(AUTHORIZED_PRINCIPAL));
    actorFilter.setUsers(usersUrnArray);
    actorFilter.setResourceOwners(false);
    actorFilter.setAllUsers(false);
    actorFilter.setAllGroups(false);
    dataHubPolicyInfo.setActors(actorFilter);

    final DataHubResourceFilter resourceFilter = new DataHubResourceFilter();
    resourceFilter.setAllResources(true);
    resourceFilter.setType("dataset");
    dataHubPolicyInfo.setResources(resourceFilter);

    ResolvedEntitySpec resourceSpec = buildEntityResolvers("dataset", RESOURCE_URN);
    // Assert unauthorized user cannot edit entity tags.
    PolicyEngine.PolicyEvaluationResult result2 =
        _policyEngine.evaluatePolicy(
            systemOperationContext,
            dataHubPolicyInfo,
            buildEntityResolvers(CORP_USER_ENTITY_NAME, "urn:li:corpuser:test"),
            "EDIT_ENTITY_TAGS",
            Optional.of(resourceSpec),
            Collections.emptyList());

    assertFalse(result2.isGranted());

    // Verify we are not making any network calls for these predicates.
    verify(_entityClient, times(0)).batchGetV2(any(), any(), any(), any());
  }

  @Test
  public void testEvaluatePolicyActorFilterGroupMatch() throws Exception {

    final DataHubPolicyInfo dataHubPolicyInfo = new DataHubPolicyInfo();
    dataHubPolicyInfo.setType(METADATA_POLICY_TYPE);
    dataHubPolicyInfo.setState(ACTIVE_POLICY_STATE);
    dataHubPolicyInfo.setPrivileges(new StringArray("EDIT_ENTITY_TAGS"));
    dataHubPolicyInfo.setDisplayName("My Test Display");
    dataHubPolicyInfo.setDescription("My test display!");
    dataHubPolicyInfo.setEditable(true);

    final DataHubActorFilter actorFilter = new DataHubActorFilter();
    final UrnArray groupsUrnArray = new UrnArray();
    groupsUrnArray.add(Urn.createFromString(AUTHORIZED_GROUP));
    actorFilter.setGroups(groupsUrnArray);
    actorFilter.setResourceOwners(false);
    actorFilter.setAllUsers(false);
    actorFilter.setAllGroups(false);
    dataHubPolicyInfo.setActors(actorFilter);

    final DataHubResourceFilter resourceFilter = new DataHubResourceFilter();
    resourceFilter.setAllResources(true);
    resourceFilter.setType("dataset");
    dataHubPolicyInfo.setResources(resourceFilter);

    ResolvedEntitySpec resourceSpec = buildEntityResolvers("dataset", RESOURCE_URN);
    // Assert authorized user can edit entity tags, because of group membership.
    PolicyEngine.PolicyEvaluationResult result1 =
        _policyEngine.evaluatePolicy(
            systemOperationContext,
            dataHubPolicyInfo,
            resolvedAuthorizedUserSpec,
            "EDIT_ENTITY_TAGS",
            Optional.of(resourceSpec),
            Collections.emptyList());
    assertTrue(result1.isGranted());

    // Verify no network calls
    verify(_entityClient, times(0)).batchGetV2(any(), any(), any(), any());
  }

  @Test
  public void testEvaluatePolicyActorFilterGroupNoMatch() throws Exception {

    final DataHubPolicyInfo dataHubPolicyInfo = new DataHubPolicyInfo();
    dataHubPolicyInfo.setType(METADATA_POLICY_TYPE);
    dataHubPolicyInfo.setState(ACTIVE_POLICY_STATE);
    dataHubPolicyInfo.setPrivileges(new StringArray("EDIT_ENTITY_TAGS"));
    dataHubPolicyInfo.setDisplayName("My Test Display");
    dataHubPolicyInfo.setDescription("My test display!");
    dataHubPolicyInfo.setEditable(true);

    final DataHubActorFilter actorFilter = new DataHubActorFilter();
    final UrnArray groupsUrnArray = new UrnArray();
    groupsUrnArray.add(Urn.createFromString(AUTHORIZED_GROUP));
    actorFilter.setGroups(groupsUrnArray);
    actorFilter.setResourceOwners(false);
    actorFilter.setAllUsers(false);
    actorFilter.setAllGroups(false);
    dataHubPolicyInfo.setActors(actorFilter);

    final DataHubResourceFilter resourceFilter = new DataHubResourceFilter();
    resourceFilter.setAllResources(true);
    resourceFilter.setType("dataset");
    dataHubPolicyInfo.setResources(resourceFilter);

    ResolvedEntitySpec resourceSpec = buildEntityResolvers("dataset", RESOURCE_URN);
    // Assert unauthorized user cannot edit entity tags.
    PolicyEngine.PolicyEvaluationResult result2 =
        _policyEngine.evaluatePolicy(
            systemOperationContext,
            dataHubPolicyInfo,
            resolvedUnauthorizedUserSpec,
            "EDIT_ENTITY_TAGS",
            Optional.of(resourceSpec),
            Collections.emptyList());
    assertFalse(result2.isGranted());

    // Verify no network calls
    verify(_entityClient, times(0)).batchGetV2(any(), any(), any(), any());
  }

  @Test
  // Write a test to verify that the policy engine is able to evaluate a policy with a role match
  public void testEvaluatePolicyActorFilterRoleMatch() throws Exception {

    final DataHubPolicyInfo dataHubPolicyInfo = new DataHubPolicyInfo();
    dataHubPolicyInfo.setType(METADATA_POLICY_TYPE);
    dataHubPolicyInfo.setState(ACTIVE_POLICY_STATE);
    dataHubPolicyInfo.setPrivileges(new StringArray("EDIT_ENTITY_TAGS"));
    dataHubPolicyInfo.setDisplayName("My Test Display");
    dataHubPolicyInfo.setDescription("My test display!");
    dataHubPolicyInfo.setEditable(true);

    final DataHubActorFilter actorFilter = new DataHubActorFilter();
    final UrnArray rolesUrnArray = new UrnArray();
    rolesUrnArray.add(Urn.createFromString("urn:li:dataHubRole:admin"));
    actorFilter.setRoles(rolesUrnArray);
    actorFilter.setResourceOwners(false);
    actorFilter.setAllUsers(false);
    actorFilter.setAllGroups(false);
    dataHubPolicyInfo.setActors(actorFilter);

    final DataHubResourceFilter resourceFilter = new DataHubResourceFilter();
    resourceFilter.setAllResources(true);
    resourceFilter.setType("dataset");
    dataHubPolicyInfo.setResources(resourceFilter);

    ResolvedEntitySpec resourceSpec = buildEntityResolvers("dataset", RESOURCE_URN);
    // Assert authorized user can edit entity tags.
    PolicyEngine.PolicyEvaluationResult authorizedResult =
        _policyEngine.evaluatePolicy(
            systemOperationContext,
            dataHubPolicyInfo,
            resolvedAuthorizedUserSpec,
            "EDIT_ENTITY_TAGS",
            Optional.of(resourceSpec),
            Collections.emptyList());

    assertTrue(authorizedResult.isGranted());

    // Verify we are only calling for roles during these requests.
    verify(_entityClient, times(1))
        .batchGetV2(
            eq(systemOperationContext),
            eq(CORP_USER_ENTITY_NAME),
            eq(Collections.singleton(authorizedUserUrn)),
            any());
  }

  @Test
  // Write a test to verify that the policy engine is able to evaluate a policy with a role match
  public void testEvaluatePolicyActorFilterNoRoleMatch() throws Exception {

    final DataHubPolicyInfo dataHubPolicyInfo = new DataHubPolicyInfo();
    dataHubPolicyInfo.setType(METADATA_POLICY_TYPE);
    dataHubPolicyInfo.setState(ACTIVE_POLICY_STATE);
    dataHubPolicyInfo.setPrivileges(new StringArray("EDIT_ENTITY_TAGS"));
    dataHubPolicyInfo.setDisplayName("My Test Display");
    dataHubPolicyInfo.setDescription("My test display!");
    dataHubPolicyInfo.setEditable(true);

    final DataHubActorFilter actorFilter = new DataHubActorFilter();
    final UrnArray rolesUrnArray = new UrnArray();
    rolesUrnArray.add(Urn.createFromString("urn:li:dataHubRole:admin"));
    actorFilter.setRoles(rolesUrnArray);
    actorFilter.setResourceOwners(false);
    actorFilter.setAllUsers(false);
    actorFilter.setAllGroups(false);
    dataHubPolicyInfo.setActors(actorFilter);

    final DataHubResourceFilter resourceFilter = new DataHubResourceFilter();
    resourceFilter.setAllResources(true);
    resourceFilter.setType("dataset");
    dataHubPolicyInfo.setResources(resourceFilter);

    ResolvedEntitySpec resourceSpec = buildEntityResolvers("dataset", RESOURCE_URN);
    // Assert authorized user can edit entity tags.
    PolicyEngine.PolicyEvaluationResult unauthorizedResult =
        _policyEngine.evaluatePolicy(
            systemOperationContext,
            dataHubPolicyInfo,
            resolvedUnauthorizedUserSpec,
            "EDIT_ENTITY_TAGS",
            Optional.of(resourceSpec),
            Collections.emptyList());

    assertFalse(unauthorizedResult.isGranted());

    // Verify we are only calling for roles during these requests.
    verify(_entityClient, times(1))
        .batchGetV2(
            eq(systemOperationContext),
            eq(CORP_USER_ENTITY_NAME),
            eq(Collections.singleton(unauthorizedUserUrn)),
            any());
  }

  @Test
  public void testEvaluatePolicyActorFilterAllUsersMatch() throws Exception {

    final DataHubPolicyInfo dataHubPolicyInfo = new DataHubPolicyInfo();
    dataHubPolicyInfo.setType(METADATA_POLICY_TYPE);
    dataHubPolicyInfo.setState(ACTIVE_POLICY_STATE);
    dataHubPolicyInfo.setPrivileges(new StringArray("EDIT_ENTITY_TAGS"));
    dataHubPolicyInfo.setDisplayName("My Test Display");
    dataHubPolicyInfo.setDescription("My test display!");
    dataHubPolicyInfo.setEditable(true);

    final DataHubActorFilter actorFilter = new DataHubActorFilter();
    actorFilter.setResourceOwners(false);
    actorFilter.setAllUsers(true);
    actorFilter.setAllGroups(false);
    dataHubPolicyInfo.setActors(actorFilter);

    final DataHubResourceFilter resourceFilter = new DataHubResourceFilter();
    resourceFilter.setAllResources(true);
    resourceFilter.setType("dataset");
    dataHubPolicyInfo.setResources(resourceFilter);

    ResolvedEntitySpec resourceSpec = buildEntityResolvers("dataset", RESOURCE_URN);
    // Assert authorized user can edit entity tags, because of group membership.
    PolicyEngine.PolicyEvaluationResult result1 =
        _policyEngine.evaluatePolicy(
            systemOperationContext,
            dataHubPolicyInfo,
            resolvedAuthorizedUserSpec,
            "EDIT_ENTITY_TAGS",
            Optional.of(resourceSpec),
            Collections.emptyList());
    assertTrue(result1.isGranted());

    // Assert unauthorized user cannot edit entity tags.
    PolicyEngine.PolicyEvaluationResult result2 =
        _policyEngine.evaluatePolicy(
            systemOperationContext,
            dataHubPolicyInfo,
            resolvedUnauthorizedUserSpec,
            "EDIT_ENTITY_TAGS",
            Optional.of(resourceSpec),
            Collections.emptyList());
    assertTrue(result2.isGranted());

    // Verify no network calls
    verify(_entityClient, times(0)).batchGetV2(any(), any(), any(), any());
  }

  @Test
  public void testEvaluatePolicyActorFilterAllGroupsMatch() throws Exception {

    final DataHubPolicyInfo dataHubPolicyInfo = new DataHubPolicyInfo();
    dataHubPolicyInfo.setType(METADATA_POLICY_TYPE);
    dataHubPolicyInfo.setState(ACTIVE_POLICY_STATE);
    dataHubPolicyInfo.setPrivileges(new StringArray("EDIT_ENTITY_TAGS"));
    dataHubPolicyInfo.setDisplayName("My Test Display");
    dataHubPolicyInfo.setDescription("My test display!");
    dataHubPolicyInfo.setEditable(true);

    final DataHubActorFilter actorFilter = new DataHubActorFilter();
    actorFilter.setResourceOwners(false);
    actorFilter.setAllUsers(false);
    actorFilter.setAllGroups(true);
    dataHubPolicyInfo.setActors(actorFilter);

    final DataHubResourceFilter resourceFilter = new DataHubResourceFilter();
    resourceFilter.setAllResources(true);
    resourceFilter.setType("dataset");
    dataHubPolicyInfo.setResources(resourceFilter);

    ResolvedEntitySpec resourceSpec = buildEntityResolvers("dataset", RESOURCE_URN);
    // Assert authorized user can edit entity tags, because of group membership.
    PolicyEngine.PolicyEvaluationResult result1 =
        _policyEngine.evaluatePolicy(
            systemOperationContext,
            dataHubPolicyInfo,
            resolvedAuthorizedUserSpec,
            "EDIT_ENTITY_TAGS",
            Optional.of(resourceSpec),
            Collections.emptyList());
    assertTrue(result1.isGranted());

    // Assert unauthorized user cannot edit entity tags.
    PolicyEngine.PolicyEvaluationResult result2 =
        _policyEngine.evaluatePolicy(
            systemOperationContext,
            dataHubPolicyInfo,
            resolvedUnauthorizedUserSpec,
            "EDIT_ENTITY_TAGS",
            Optional.of(resourceSpec),
            Collections.emptyList());
    assertFalse(result2.isGranted());

    // Verify no network calls
    verify(_entityClient, times(0)).batchGetV2(any(), any(), any(), any());
  }

  @Test
  public void testEvaluatePolicyActorFilterUserResourceOwnersMatch() throws Exception {

    final DataHubPolicyInfo dataHubPolicyInfo = new DataHubPolicyInfo();
    dataHubPolicyInfo.setType(METADATA_POLICY_TYPE);
    dataHubPolicyInfo.setState(ACTIVE_POLICY_STATE);
    dataHubPolicyInfo.setPrivileges(new StringArray("EDIT_ENTITY_TAGS"));
    dataHubPolicyInfo.setDisplayName("My Test Display");
    dataHubPolicyInfo.setDescription("My test display!");
    dataHubPolicyInfo.setEditable(true);

    final DataHubActorFilter actorFilter = new DataHubActorFilter();
    actorFilter.setResourceOwners(true);
    actorFilter.setAllUsers(false);
    actorFilter.setAllGroups(false);
    dataHubPolicyInfo.setActors(actorFilter);

    final DataHubResourceFilter resourceFilter = new DataHubResourceFilter();
    resourceFilter.setAllResources(true);
    resourceFilter.setType("dataset");
    dataHubPolicyInfo.setResources(resourceFilter);

    final EntityResponse entityResponse = new EntityResponse();
    final EnvelopedAspectMap aspectMap = new EnvelopedAspectMap();
    aspectMap.put(
        OWNERSHIP_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(createOwnershipAspect(true, false).data())));
    entityResponse.setAspects(aspectMap);
    when(_entityClient.getV2(
            eq(systemOperationContext),
            eq(resourceUrn.getEntityType()),
            eq(resourceUrn),
            eq(Collections.singleton(Constants.OWNERSHIP_ASPECT_NAME))))
        .thenReturn(entityResponse);

    ResolvedEntitySpec resourceSpec =
        buildEntityResolvers(
            "dataset",
            RESOURCE_URN,
            ImmutableSet.of(AUTHORIZED_PRINCIPAL),
            Collections.emptySet(),
            Collections.emptySet(),
            Collections.emptySet(),
            Collections.emptySet());
    // Assert authorized user can edit entity tags, because he is a user owner.
    PolicyEngine.PolicyEvaluationResult result1 =
        _policyEngine.evaluatePolicy(
            systemOperationContext,
            dataHubPolicyInfo,
            resolvedAuthorizedUserSpec,
            "EDIT_ENTITY_TAGS",
            Optional.of(resourceSpec),
            Collections.emptyList());
    assertTrue(result1.isGranted());

    // Verify no network calls
    verify(_entityClient, times(0)).batchGetV2(any(), any(), any(), any());
  }

  @Test
  public void testEvaluatePolicyActorFilterUserResourceOwnersTypeMatch() throws Exception {

    final DataHubPolicyInfo dataHubPolicyInfo = new DataHubPolicyInfo();
    dataHubPolicyInfo.setType(METADATA_POLICY_TYPE);
    dataHubPolicyInfo.setState(ACTIVE_POLICY_STATE);
    dataHubPolicyInfo.setPrivileges(new StringArray("EDIT_ENTITY_TAGS"));
    dataHubPolicyInfo.setDisplayName("My Test Display");
    dataHubPolicyInfo.setDescription("My test display!");
    dataHubPolicyInfo.setEditable(true);

    final DataHubActorFilter actorFilter = new DataHubActorFilter();
    actorFilter.setResourceOwners(true);
    actorFilter.setAllUsers(false);
    actorFilter.setAllGroups(false);
    actorFilter.setResourceOwnersTypes(
        new UrnArray(ImmutableList.of(Urn.createFromString(OWNERSHIP_TYPE_URN))));
    dataHubPolicyInfo.setActors(actorFilter);

    final DataHubResourceFilter resourceFilter = new DataHubResourceFilter();
    resourceFilter.setAllResources(true);
    resourceFilter.setType("dataset");
    dataHubPolicyInfo.setResources(resourceFilter);

    final EntityResponse entityResponse = new EntityResponse();
    final EnvelopedAspectMap aspectMap = new EnvelopedAspectMap();
    aspectMap.put(
        OWNERSHIP_ASPECT_NAME,
        new EnvelopedAspect()
            .setValue(new Aspect(createOwnershipAspectWithTypeUrn(OWNERSHIP_TYPE_URN).data())));
    entityResponse.setAspects(aspectMap);
    when(_entityClient.getV2(
            eq(systemOperationContext),
            eq(resourceUrn.getEntityType()),
            eq(resourceUrn),
            eq(Collections.singleton(Constants.OWNERSHIP_ASPECT_NAME))))
        .thenReturn(entityResponse);

    ResolvedEntitySpec resourceSpec =
        buildEntityResolvers(
            "dataset",
            RESOURCE_URN,
            ImmutableSet.of(AUTHORIZED_PRINCIPAL),
            Collections.emptySet(),
            Collections.emptySet(),
            Collections.emptySet(),
            Collections.emptySet());

    PolicyEngine.PolicyEvaluationResult result1 =
        _policyEngine.evaluatePolicy(
            systemOperationContext,
            dataHubPolicyInfo,
            resolvedAuthorizedUserSpec,
            "EDIT_ENTITY_TAGS",
            Optional.of(resourceSpec),
            Collections.emptyList());
    assertTrue(result1.isGranted());

    // Verify no network calls
    verify(_entityClient, times(0)).batchGetV2(any(), any(), any(), any());
  }

  @Test
  public void testEvaluatePolicyActorFilterUserResourceOwnersTypeNoMatch() throws Exception {

    final DataHubPolicyInfo dataHubPolicyInfo = new DataHubPolicyInfo();
    dataHubPolicyInfo.setType(METADATA_POLICY_TYPE);
    dataHubPolicyInfo.setState(ACTIVE_POLICY_STATE);
    dataHubPolicyInfo.setPrivileges(new StringArray("EDIT_ENTITY_TAGS"));
    dataHubPolicyInfo.setDisplayName("My Test Display");
    dataHubPolicyInfo.setDescription("My test display!");
    dataHubPolicyInfo.setEditable(true);

    final DataHubActorFilter actorFilter = new DataHubActorFilter();
    actorFilter.setResourceOwners(true);
    actorFilter.setAllUsers(false);
    actorFilter.setAllGroups(false);
    actorFilter.setResourceOwnersTypes(
        new UrnArray(ImmutableList.of(Urn.createFromString(OWNERSHIP_TYPE_URN))));
    dataHubPolicyInfo.setActors(actorFilter);

    final DataHubResourceFilter resourceFilter = new DataHubResourceFilter();
    resourceFilter.setAllResources(true);
    resourceFilter.setType("dataset");
    dataHubPolicyInfo.setResources(resourceFilter);

    final EntityResponse entityResponse = new EntityResponse();
    final EnvelopedAspectMap aspectMap = new EnvelopedAspectMap();
    aspectMap.put(
        OWNERSHIP_ASPECT_NAME,
        new EnvelopedAspect()
            .setValue(
                new Aspect(createOwnershipAspectWithTypeUrn(OTHER_OWNERSHIP_TYPE_URN).data())));
    entityResponse.setAspects(aspectMap);
    when(_entityClient.getV2(
            eq(systemOperationContext),
            eq(resourceUrn.getEntityType()),
            eq(resourceUrn),
            eq(Collections.singleton(Constants.OWNERSHIP_ASPECT_NAME))))
        .thenReturn(entityResponse);

    ResolvedEntitySpec resourceSpec =
        buildEntityResolvers(
            "dataset",
            RESOURCE_URN,
            ImmutableSet.of(AUTHORIZED_PRINCIPAL),
            Collections.emptySet(),
            Collections.emptySet(),
            Collections.emptySet(),
            Collections.emptySet());

    PolicyEngine.PolicyEvaluationResult result1 =
        _policyEngine.evaluatePolicy(
            systemOperationContext,
            dataHubPolicyInfo,
            resolvedAuthorizedUserSpec,
            "EDIT_ENTITY_TAGS",
            Optional.of(resourceSpec),
            Collections.emptyList());
    assertFalse(result1.isGranted());

    // Verify no network calls
    verify(_entityClient, times(0)).batchGetV2(any(), any(), any(), any());
  }

  @Test
  public void testEvaluatePolicyActorFilterGroupResourceOwnersMatch() throws Exception {

    final DataHubPolicyInfo dataHubPolicyInfo = new DataHubPolicyInfo();
    dataHubPolicyInfo.setType(METADATA_POLICY_TYPE);
    dataHubPolicyInfo.setState(ACTIVE_POLICY_STATE);
    dataHubPolicyInfo.setPrivileges(new StringArray("EDIT_ENTITY_TAGS"));
    dataHubPolicyInfo.setDisplayName("My Test Display");
    dataHubPolicyInfo.setDescription("My test display!");
    dataHubPolicyInfo.setEditable(true);

    final DataHubActorFilter actorFilter = new DataHubActorFilter();
    actorFilter.setResourceOwners(true);
    actorFilter.setAllUsers(false);
    actorFilter.setAllGroups(false);
    dataHubPolicyInfo.setActors(actorFilter);

    final DataHubResourceFilter resourceFilter = new DataHubResourceFilter();
    resourceFilter.setAllResources(true);
    resourceFilter.setType("dataset");
    dataHubPolicyInfo.setResources(resourceFilter);

    final EntityResponse entityResponse = new EntityResponse();
    final EnvelopedAspectMap aspectMap = new EnvelopedAspectMap();
    aspectMap.put(
        OWNERSHIP_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(createOwnershipAspect(false, true).data())));
    entityResponse.setAspects(aspectMap);
    when(_entityClient.getV2(
            eq(systemOperationContext),
            eq(resourceUrn.getEntityType()),
            eq(resourceUrn),
            eq(Collections.singleton(Constants.OWNERSHIP_ASPECT_NAME))))
        .thenReturn(entityResponse);

    ResolvedEntitySpec resourceSpec =
        buildEntityResolvers(
            "dataset",
            RESOURCE_URN,
            ImmutableSet.of(AUTHORIZED_GROUP),
            Collections.emptySet(),
            Collections.emptySet(),
            Collections.emptySet(),
            Collections.emptySet());
    // Assert authorized user can edit entity tags, because he is a user owner.
    PolicyEngine.PolicyEvaluationResult result1 =
        _policyEngine.evaluatePolicy(
            systemOperationContext,
            dataHubPolicyInfo,
            resolvedAuthorizedUserSpec,
            "EDIT_ENTITY_TAGS",
            Optional.of(resourceSpec),
            Collections.emptyList());
    assertTrue(result1.isGranted());

    // Verify no network calls
    verify(_entityClient, times(0)).batchGetV2(any(), any(), any(), any());
  }

  @Test
  public void testEvaluatePolicyActorFilterGroupResourceOwnersNoMatch() throws Exception {
    final DataHubPolicyInfo dataHubPolicyInfo = new DataHubPolicyInfo();
    dataHubPolicyInfo.setType(METADATA_POLICY_TYPE);
    dataHubPolicyInfo.setState(ACTIVE_POLICY_STATE);
    dataHubPolicyInfo.setPrivileges(new StringArray("EDIT_ENTITY_TAGS"));
    dataHubPolicyInfo.setDisplayName("My Test Display");
    dataHubPolicyInfo.setDescription("My test display!");
    dataHubPolicyInfo.setEditable(true);

    final DataHubActorFilter actorFilter = new DataHubActorFilter();
    actorFilter.setResourceOwners(true);
    actorFilter.setAllUsers(false);
    actorFilter.setAllGroups(false);
    dataHubPolicyInfo.setActors(actorFilter);

    final DataHubResourceFilter resourceFilter = new DataHubResourceFilter();
    resourceFilter.setAllResources(true);
    resourceFilter.setType("dataset");
    dataHubPolicyInfo.setResources(resourceFilter);

    ResolvedEntitySpec resourceSpec = buildEntityResolvers("dataset", RESOURCE_URN);
    // Assert unauthorized user cannot edit entity tags.
    PolicyEngine.PolicyEvaluationResult result2 =
        _policyEngine.evaluatePolicy(
            systemOperationContext,
            dataHubPolicyInfo,
            resolvedUnauthorizedUserSpec,
            "EDIT_ENTITY_TAGS",
            Optional.of(resourceSpec),
            Collections.emptyList());
    assertFalse(result2.isGranted());

    // Verify no network calls
    verify(_entityClient, times(0)).batchGetV2(any(), any(), any(), any());
  }

  @Test
  public void testEvaluatePolicyResourceFilterAllResourcesMatch() throws Exception {
    final DataHubPolicyInfo dataHubPolicyInfo = new DataHubPolicyInfo();
    dataHubPolicyInfo.setType(METADATA_POLICY_TYPE);
    dataHubPolicyInfo.setState(ACTIVE_POLICY_STATE);
    dataHubPolicyInfo.setPrivileges(new StringArray("EDIT_ENTITY_TAGS"));
    dataHubPolicyInfo.setDisplayName("My Test Display");
    dataHubPolicyInfo.setDescription("My test display!");
    dataHubPolicyInfo.setEditable(true);

    final DataHubActorFilter actorFilter = new DataHubActorFilter();
    actorFilter.setResourceOwners(true);
    actorFilter.setAllUsers(true);
    actorFilter.setAllGroups(true);
    dataHubPolicyInfo.setActors(actorFilter);

    final DataHubResourceFilter resourceFilter = new DataHubResourceFilter();
    resourceFilter.setAllResources(true);
    resourceFilter.setType("dataset");
    dataHubPolicyInfo.setResources(resourceFilter);

    ResolvedEntitySpec resourceSpec =
        buildEntityResolvers(
            "dataset", "urn:li:dataset:random"); // A dataset Authorized principal _does not own_.
    PolicyEngine.PolicyEvaluationResult result =
        _policyEngine.evaluatePolicy(
            systemOperationContext,
            dataHubPolicyInfo,
            resolvedAuthorizedUserSpec,
            "EDIT_ENTITY_TAGS",
            Optional.of(resourceSpec),
            Collections.emptyList());
    assertTrue(result.isGranted());

    // Verify no network calls
    verify(_entityClient, times(0)).batchGetV2(any(), any(), any(), any());
  }

  @Test
  public void testEvaluatePolicyResourceFilterAllResourcesNoMatch() throws Exception {
    final DataHubPolicyInfo dataHubPolicyInfo = new DataHubPolicyInfo();
    dataHubPolicyInfo.setType(METADATA_POLICY_TYPE);
    dataHubPolicyInfo.setState(ACTIVE_POLICY_STATE);
    dataHubPolicyInfo.setPrivileges(new StringArray("EDIT_ENTITY_TAGS"));
    dataHubPolicyInfo.setDisplayName("My Test Display");
    dataHubPolicyInfo.setDescription("My test display!");
    dataHubPolicyInfo.setEditable(true);

    final DataHubActorFilter actorFilter = new DataHubActorFilter();
    actorFilter.setResourceOwners(true);
    actorFilter.setAllUsers(true);
    actorFilter.setAllGroups(true);
    dataHubPolicyInfo.setActors(actorFilter);

    final DataHubResourceFilter resourceFilter = new DataHubResourceFilter();
    resourceFilter.setAllResources(true);
    resourceFilter.setType("dataset");
    dataHubPolicyInfo.setResources(resourceFilter);

    ResolvedEntitySpec resourceSpec =
        buildEntityResolvers("chart", RESOURCE_URN); // Notice: Not a dataset.
    PolicyEngine.PolicyEvaluationResult result =
        _policyEngine.evaluatePolicy(
            systemOperationContext,
            dataHubPolicyInfo,
            resolvedAuthorizedUserSpec,
            "EDIT_ENTITY_TAGS",
            Optional.of(resourceSpec),
            Collections.emptyList());
    assertFalse(result.isGranted());

    // Verify no network calls
    verify(_entityClient, times(0)).batchGetV2(any(), any(), any(), any());
  }

  @Test
  public void testEvaluatePolicyResourceFilterSpecificResourceMatchLegacy() throws Exception {
    final DataHubPolicyInfo dataHubPolicyInfo = new DataHubPolicyInfo();
    dataHubPolicyInfo.setType(METADATA_POLICY_TYPE);
    dataHubPolicyInfo.setState(ACTIVE_POLICY_STATE);
    dataHubPolicyInfo.setPrivileges(new StringArray("EDIT_ENTITY_TAGS"));
    dataHubPolicyInfo.setDisplayName("My Test Display");
    dataHubPolicyInfo.setDescription("My test display!");
    dataHubPolicyInfo.setEditable(true);

    final DataHubActorFilter actorFilter = new DataHubActorFilter();
    actorFilter.setResourceOwners(true);
    actorFilter.setAllUsers(true);
    actorFilter.setAllGroups(true);
    dataHubPolicyInfo.setActors(actorFilter);

    final DataHubResourceFilter resourceFilter = new DataHubResourceFilter();
    resourceFilter.setAllResources(false);
    resourceFilter.setType("dataset");

    StringArray resourceUrns = new StringArray();
    resourceUrns.add(RESOURCE_URN); // Filter applies to specific resource.
    resourceFilter.setResources(resourceUrns);
    dataHubPolicyInfo.setResources(resourceFilter);

    ResolvedEntitySpec resourceSpec = buildEntityResolvers("dataset", RESOURCE_URN);
    PolicyEngine.PolicyEvaluationResult result =
        _policyEngine.evaluatePolicy(
            systemOperationContext,
            dataHubPolicyInfo,
            resolvedAuthorizedUserSpec,
            "EDIT_ENTITY_TAGS",
            Optional.of(resourceSpec),
            Collections.emptyList());
    assertTrue(result.isGranted());

    // Verify no network calls
    verify(_entityClient, times(0)).batchGetV2(any(), any(), any(), any());
  }

  @Test
  public void testEvaluatePolicyResourceFilterSpecificResourceMatchLegacyWithNoType()
      throws Exception {
    final DataHubPolicyInfo dataHubPolicyInfo = new DataHubPolicyInfo();
    dataHubPolicyInfo.setType(METADATA_POLICY_TYPE);
    dataHubPolicyInfo.setState(ACTIVE_POLICY_STATE);
    dataHubPolicyInfo.setPrivileges(new StringArray("EDIT_ENTITY_TAGS"));
    dataHubPolicyInfo.setDisplayName("My Test Display");
    dataHubPolicyInfo.setDescription("My test display!");
    dataHubPolicyInfo.setEditable(true);

    final DataHubActorFilter actorFilter = new DataHubActorFilter();
    actorFilter.setResourceOwners(true);
    actorFilter.setAllUsers(true);
    actorFilter.setAllGroups(true);
    dataHubPolicyInfo.setActors(actorFilter);

    final DataHubResourceFilter resourceFilter = new DataHubResourceFilter();
    resourceFilter.setAllResources(true);

    ResolvedEntitySpec resourceSpec = buildEntityResolvers("dataset", RESOURCE_URN);
    PolicyEngine.PolicyEvaluationResult result =
        _policyEngine.evaluatePolicy(
            systemOperationContext,
            dataHubPolicyInfo,
            resolvedAuthorizedUserSpec,
            "EDIT_ENTITY_TAGS",
            Optional.of(resourceSpec),
            Collections.emptyList());
    assertTrue(result.isGranted());

    StringArray resourceUrns = new StringArray();
    resourceUrns.add(RESOURCE_URN); // Filter applies to specific resource.
    resourceFilter.setResources(resourceUrns);
    resourceFilter.setAllResources(false); // This time should match due to RESOURCE_URN
    dataHubPolicyInfo.setResources(resourceFilter);

    resourceSpec = buildEntityResolvers("dataset", RESOURCE_URN);
    result =
        _policyEngine.evaluatePolicy(
            systemOperationContext,
            dataHubPolicyInfo,
            resolvedAuthorizedUserSpec,
            "EDIT_ENTITY_TAGS",
            Optional.of(resourceSpec),
            Collections.emptyList());
    assertTrue(result.isGranted());

    // Verify no network calls
    verify(_entityClient, times(0)).batchGetV2(any(), any(), any(), any());
  }

  @Test
  public void testEvaluatePolicyResourceFilterSpecificResourceMatch() throws Exception {
    final DataHubPolicyInfo dataHubPolicyInfo = new DataHubPolicyInfo();
    dataHubPolicyInfo.setType(METADATA_POLICY_TYPE);
    dataHubPolicyInfo.setState(ACTIVE_POLICY_STATE);
    dataHubPolicyInfo.setPrivileges(new StringArray("EDIT_ENTITY_TAGS"));
    dataHubPolicyInfo.setDisplayName("My Test Display");
    dataHubPolicyInfo.setDescription("My test display!");
    dataHubPolicyInfo.setEditable(true);

    final DataHubActorFilter actorFilter = new DataHubActorFilter();
    actorFilter.setResourceOwners(true);
    actorFilter.setAllUsers(true);
    actorFilter.setAllGroups(true);
    dataHubPolicyInfo.setActors(actorFilter);

    final DataHubResourceFilter resourceFilter = new DataHubResourceFilter();
    resourceFilter.setFilter(
        FilterUtils.newFilter(
            ImmutableMap.of(
                EntityFieldType.TYPE,
                Collections.singletonList("dataset"),
                EntityFieldType.URN,
                Collections.singletonList(RESOURCE_URN))));
    dataHubPolicyInfo.setResources(resourceFilter);

    ResolvedEntitySpec resourceSpec = buildEntityResolvers("dataset", RESOURCE_URN);
    PolicyEngine.PolicyEvaluationResult result =
        _policyEngine.evaluatePolicy(
            systemOperationContext,
            dataHubPolicyInfo,
            resolvedAuthorizedUserSpec,
            "EDIT_ENTITY_TAGS",
            Optional.of(resourceSpec),
            Collections.emptyList());
    assertTrue(result.isGranted());

    // Verify no network calls
    verify(_entityClient, times(0)).batchGetV2(any(), any(), any(), any());
  }

  @Test
  public void testEvaluatePolicyResourceFilterSpecificResourceNoMatch() throws Exception {
    final DataHubPolicyInfo dataHubPolicyInfo = new DataHubPolicyInfo();
    dataHubPolicyInfo.setType(METADATA_POLICY_TYPE);
    dataHubPolicyInfo.setState(ACTIVE_POLICY_STATE);
    dataHubPolicyInfo.setPrivileges(new StringArray("EDIT_ENTITY_TAGS"));
    dataHubPolicyInfo.setDisplayName("My Test Display");
    dataHubPolicyInfo.setDescription("My test display!");
    dataHubPolicyInfo.setEditable(true);

    final DataHubActorFilter actorFilter = new DataHubActorFilter();
    actorFilter.setResourceOwners(true);
    actorFilter.setAllUsers(true);
    actorFilter.setAllGroups(true);
    dataHubPolicyInfo.setActors(actorFilter);

    final DataHubResourceFilter resourceFilter = new DataHubResourceFilter();
    resourceFilter.setFilter(
        FilterUtils.newFilter(
            ImmutableMap.of(
                EntityFieldType.TYPE,
                Collections.singletonList("dataset"),
                EntityFieldType.URN,
                Collections.singletonList(RESOURCE_URN))));
    dataHubPolicyInfo.setResources(resourceFilter);

    ResolvedEntitySpec resourceSpec =
        buildEntityResolvers(
            "dataset", "urn:li:dataset:random"); // A resource not covered by the policy.
    PolicyEngine.PolicyEvaluationResult result =
        _policyEngine.evaluatePolicy(
            systemOperationContext,
            dataHubPolicyInfo,
            resolvedAuthorizedUserSpec,
            "EDIT_ENTITY_TAGS",
            Optional.of(resourceSpec),
            Collections.emptyList());
    assertFalse(result.isGranted());

    // Verify no network calls
    verify(_entityClient, times(0)).batchGetV2(any(), any(), any(), any());
  }

  @Test
  public void testEvaluatePolicyResourceFilterSpecificResourceLegacyNoMatchWithNoType()
      throws Exception {
    final DataHubPolicyInfo dataHubPolicyInfo = new DataHubPolicyInfo();
    dataHubPolicyInfo.setType(METADATA_POLICY_TYPE);
    dataHubPolicyInfo.setState(ACTIVE_POLICY_STATE);
    dataHubPolicyInfo.setPrivileges(new StringArray("EDIT_ENTITY_TAGS"));
    dataHubPolicyInfo.setDisplayName("My Test Display");
    dataHubPolicyInfo.setDescription("My test display!");
    dataHubPolicyInfo.setEditable(true);

    final DataHubActorFilter actorFilter = new DataHubActorFilter();
    actorFilter.setResourceOwners(true);
    actorFilter.setAllUsers(true);
    actorFilter.setAllGroups(true);
    dataHubPolicyInfo.setActors(actorFilter);

    final DataHubResourceFilter resourceFilter = new DataHubResourceFilter();
    StringArray resourceUrns = new StringArray();
    resourceUrns.add(RESOURCE_URN); // Filter applies to specific resource.
    resourceFilter.setResources(resourceUrns);
    dataHubPolicyInfo.setResources(resourceFilter);

    ResolvedEntitySpec resourceSpec =
        buildEntityResolvers(
            "dataset", "urn:li:dataset:random"); // A resource not covered by the policy.
    PolicyEngine.PolicyEvaluationResult result =
        _policyEngine.evaluatePolicy(
            systemOperationContext,
            dataHubPolicyInfo,
            resolvedAuthorizedUserSpec,
            "EDIT_ENTITY_TAGS",
            Optional.of(resourceSpec),
            Collections.emptyList());
    assertFalse(result.isGranted());

    resourceFilter.setAllResources(true);
    dataHubPolicyInfo.setResources(resourceFilter);

    resourceSpec =
        buildEntityResolvers(
            "dataset", "urn:li:dataset:random"); // A resource not covered by the policy.
    result =
        _policyEngine.evaluatePolicy(
            systemOperationContext,
            dataHubPolicyInfo,
            resolvedAuthorizedUserSpec,
            "EDIT_ENTITY_TAGS",
            Optional.of(resourceSpec),
            Collections.emptyList());
    assertTrue(result.isGranted()); // Due to allResources set to true

    // Verify no network calls
    verify(_entityClient, times(0)).batchGetV2(any(), any(), any(), any());
  }

  @Test
  public void testEvaluatePolicyResourceFilterResourceUrnStartsWithMatch() throws Exception {
    final DataHubPolicyInfo dataHubPolicyInfo = new DataHubPolicyInfo();
    dataHubPolicyInfo.setType(METADATA_POLICY_TYPE);
    dataHubPolicyInfo.setState(ACTIVE_POLICY_STATE);
    dataHubPolicyInfo.setPrivileges(new StringArray("EDIT_ENTITY_TAGS"));
    dataHubPolicyInfo.setDisplayName("My Test Display");
    dataHubPolicyInfo.setDescription("My test display!");
    dataHubPolicyInfo.setEditable(true);

    final DataHubActorFilter actorFilter = new DataHubActorFilter();
    actorFilter.setResourceOwners(true);
    actorFilter.setAllUsers(true);
    actorFilter.setAllGroups(true);
    dataHubPolicyInfo.setActors(actorFilter);

    final DataHubResourceFilter resourceFilter = new DataHubResourceFilter();
    PolicyMatchCriterion policyMatchCriterion =
        FilterUtils.newCriterion(
            EntityFieldType.URN,
            Collections.singletonList("urn:li:dataset:te"),
            PolicyMatchCondition.STARTS_WITH);

    resourceFilter.setFilter(
        new PolicyMatchFilter()
            .setCriteria(
                new PolicyMatchCriterionArray(Collections.singleton(policyMatchCriterion))));
    dataHubPolicyInfo.setResources(resourceFilter);

    ResolvedEntitySpec resourceSpec = buildEntityResolvers("dataset", RESOURCE_URN);
    PolicyEngine.PolicyEvaluationResult result =
        _policyEngine.evaluatePolicy(
            systemOperationContext,
            dataHubPolicyInfo,
            resolvedAuthorizedUserSpec,
            "EDIT_ENTITY_TAGS",
            Optional.of(resourceSpec),
            Collections.emptyList());
    assertTrue(result.isGranted());

    // Verify no network calls
    verify(_entityClient, times(0)).batchGetV2(any(), any(), any(), any());
  }

  @Test
  public void testEvaluatePolicyResourceFilterResourceUrnStartsWithNoMatch() throws Exception {
    final DataHubPolicyInfo dataHubPolicyInfo = new DataHubPolicyInfo();
    dataHubPolicyInfo.setType(METADATA_POLICY_TYPE);
    dataHubPolicyInfo.setState(ACTIVE_POLICY_STATE);
    dataHubPolicyInfo.setPrivileges(new StringArray("EDIT_ENTITY_TAGS"));
    dataHubPolicyInfo.setDisplayName("My Test Display");
    dataHubPolicyInfo.setDescription("My test display!");
    dataHubPolicyInfo.setEditable(true);

    final DataHubActorFilter actorFilter = new DataHubActorFilter();
    actorFilter.setResourceOwners(true);
    actorFilter.setAllUsers(true);
    actorFilter.setAllGroups(true);
    dataHubPolicyInfo.setActors(actorFilter);

    final DataHubResourceFilter resourceFilter = new DataHubResourceFilter();
    PolicyMatchCriterion policyMatchCriterion =
        FilterUtils.newCriterion(
            EntityFieldType.URN,
            Collections.singletonList("urn:li:dataset:other"),
            PolicyMatchCondition.STARTS_WITH);

    resourceFilter.setFilter(
        new PolicyMatchFilter()
            .setCriteria(
                new PolicyMatchCriterionArray(Collections.singleton(policyMatchCriterion))));
    dataHubPolicyInfo.setResources(resourceFilter);

    ResolvedEntitySpec resourceSpec = buildEntityResolvers("dataset", RESOURCE_URN);
    PolicyEngine.PolicyEvaluationResult result =
        _policyEngine.evaluatePolicy(
            systemOperationContext,
            dataHubPolicyInfo,
            resolvedAuthorizedUserSpec,
            "EDIT_ENTITY_TAGS",
            Optional.of(resourceSpec),
            Collections.emptyList());
    assertFalse(result.isGranted());

    // Verify no network calls
    verify(_entityClient, times(0)).batchGetV2(any(), any(), any(), any());
  }

  @Test
  public void testEvaluatePolicyResourceFilterSpecificResourceMatchDomain() throws Exception {
    final DataHubPolicyInfo dataHubPolicyInfo = new DataHubPolicyInfo();
    dataHubPolicyInfo.setType(METADATA_POLICY_TYPE);
    dataHubPolicyInfo.setState(ACTIVE_POLICY_STATE);
    dataHubPolicyInfo.setPrivileges(new StringArray("EDIT_ENTITY_TAGS"));
    dataHubPolicyInfo.setDisplayName("My Test Display");
    dataHubPolicyInfo.setDescription("My test display!");
    dataHubPolicyInfo.setEditable(true);

    final DataHubActorFilter actorFilter = new DataHubActorFilter();
    actorFilter.setResourceOwners(true);
    actorFilter.setAllUsers(true);
    actorFilter.setAllGroups(true);
    dataHubPolicyInfo.setActors(actorFilter);

    final DataHubResourceFilter resourceFilter = new DataHubResourceFilter();
    resourceFilter.setFilter(
        FilterUtils.newFilter(
            ImmutableMap.of(
                EntityFieldType.TYPE,
                Collections.singletonList("dataset"),
                EntityFieldType.DOMAIN,
                Collections.singletonList(DOMAIN_URN))));
    dataHubPolicyInfo.setResources(resourceFilter);

    ResolvedEntitySpec resourceSpec =
        buildEntityResolvers(
            "dataset",
            RESOURCE_URN,
            Collections.emptySet(),
            Collections.singleton(DOMAIN_URN),
            Collections.emptySet(),
            Collections.emptySet(),
            Collections.emptySet());
    PolicyEngine.PolicyEvaluationResult result =
        _policyEngine.evaluatePolicy(
            systemOperationContext,
            dataHubPolicyInfo,
            resolvedAuthorizedUserSpec,
            "EDIT_ENTITY_TAGS",
            Optional.of(resourceSpec),
            Collections.emptyList());
    assertTrue(result.isGranted());

    // Verify no network calls
    verify(_entityClient, times(0)).batchGetV2(any(), any(), any(), any());
  }

  @Test
  public void testEvaluatePolicyResourceFilterSpecificResourceNoMatchDomain() throws Exception {
    final DataHubPolicyInfo dataHubPolicyInfo = new DataHubPolicyInfo();
    dataHubPolicyInfo.setType(METADATA_POLICY_TYPE);
    dataHubPolicyInfo.setState(ACTIVE_POLICY_STATE);
    dataHubPolicyInfo.setPrivileges(new StringArray("EDIT_ENTITY_TAGS"));
    dataHubPolicyInfo.setDisplayName("My Test Display");
    dataHubPolicyInfo.setDescription("My test display!");
    dataHubPolicyInfo.setEditable(true);

    final DataHubActorFilter actorFilter = new DataHubActorFilter();
    actorFilter.setResourceOwners(true);
    actorFilter.setAllUsers(true);
    actorFilter.setAllGroups(true);
    dataHubPolicyInfo.setActors(actorFilter);

    final DataHubResourceFilter resourceFilter = new DataHubResourceFilter();
    resourceFilter.setFilter(
        FilterUtils.newFilter(
            ImmutableMap.of(
                EntityFieldType.TYPE,
                Collections.singletonList("dataset"),
                EntityFieldType.DOMAIN,
                Collections.singletonList(DOMAIN_URN))));
    dataHubPolicyInfo.setResources(resourceFilter);

    ResolvedEntitySpec resourceSpec =
        buildEntityResolvers(
            "dataset",
            RESOURCE_URN,
            Collections.emptySet(),
            Collections.singleton("urn:li:domain:domain2"),
            Collections.emptySet(),
            Collections.emptySet(),
            Collections.emptySet()); // Domain doesn't match
    PolicyEngine.PolicyEvaluationResult result =
        _policyEngine.evaluatePolicy(
            systemOperationContext,
            dataHubPolicyInfo,
            resolvedAuthorizedUserSpec,
            "EDIT_ENTITY_TAGS",
            Optional.of(resourceSpec),
            Collections.emptyList());
    assertFalse(result.isGranted());

    // Verify no network calls
    verify(_entityClient, times(0)).batchGetV2(any(), any(), any(), any());
  }

  @Test
  public void testEvaluatePolicyResourceFilterSpecificResourceMatchContainer() throws Exception {
    final DataHubPolicyInfo dataHubPolicyInfo = new DataHubPolicyInfo();
    dataHubPolicyInfo.setType(METADATA_POLICY_TYPE);
    dataHubPolicyInfo.setState(ACTIVE_POLICY_STATE);
    dataHubPolicyInfo.setPrivileges(new StringArray("EDIT_ENTITY_TAGS"));
    dataHubPolicyInfo.setDisplayName("My Test Display");
    dataHubPolicyInfo.setDescription("My test display!");
    dataHubPolicyInfo.setEditable(true);

    final DataHubActorFilter actorFilter = new DataHubActorFilter();
    actorFilter.setResourceOwners(true);
    actorFilter.setAllUsers(true);
    actorFilter.setAllGroups(true);
    dataHubPolicyInfo.setActors(actorFilter);

    final DataHubResourceFilter resourceFilter = new DataHubResourceFilter();
    resourceFilter.setFilter(
        FilterUtils.newFilter(
            ImmutableMap.of(
                EntityFieldType.TYPE,
                Collections.singletonList("dataset"),
                EntityFieldType.CONTAINER,
                Collections.singletonList(CONTAINER_URN))));
    dataHubPolicyInfo.setResources(resourceFilter);

    ResolvedEntitySpec resourceSpec =
        buildEntityResolvers(
            "dataset",
            RESOURCE_URN,
            Collections.emptySet(),
            Collections.emptySet(),
            Collections.singleton(CONTAINER_URN),
            Collections.emptySet(),
            Collections.emptySet());
    PolicyEngine.PolicyEvaluationResult result =
        _policyEngine.evaluatePolicy(
            systemOperationContext,
            dataHubPolicyInfo,
            resolvedAuthorizedUserSpec,
            "EDIT_ENTITY_TAGS",
            Optional.of(resourceSpec),
            Collections.emptyList());
    assertTrue(result.isGranted());

    // Verify no network calls
    verify(_entityClient, times(0)).getV2(any(), any(), any(), any());
  }

  @Test
  public void testEvaluatePolicyResourceFilterSpecificResourceNoMatchContainer() throws Exception {
    final DataHubPolicyInfo dataHubPolicyInfo = new DataHubPolicyInfo();
    dataHubPolicyInfo.setType(METADATA_POLICY_TYPE);
    dataHubPolicyInfo.setState(ACTIVE_POLICY_STATE);
    dataHubPolicyInfo.setPrivileges(new StringArray("EDIT_ENTITY_TAGS"));
    dataHubPolicyInfo.setDisplayName("My Test Display");
    dataHubPolicyInfo.setDescription("My test display!");
    dataHubPolicyInfo.setEditable(true);

    final DataHubActorFilter actorFilter = new DataHubActorFilter();
    actorFilter.setResourceOwners(true);
    actorFilter.setAllUsers(true);
    actorFilter.setAllGroups(true);
    dataHubPolicyInfo.setActors(actorFilter);

    final DataHubResourceFilter resourceFilter = new DataHubResourceFilter();
    resourceFilter.setFilter(
        FilterUtils.newFilter(
            ImmutableMap.of(
                EntityFieldType.TYPE,
                Collections.singletonList("dataset"),
                EntityFieldType.CONTAINER,
                Collections.singletonList(CONTAINER_URN))));
    dataHubPolicyInfo.setResources(resourceFilter);

    ResolvedEntitySpec resourceSpec =
        buildEntityResolvers(
            "dataset",
            RESOURCE_URN,
            Collections.emptySet(),
            Collections.emptySet(),
            Collections.singleton("urn:li:container:notExists"),
            Collections.emptySet(),
            Collections.emptySet());
    PolicyEngine.PolicyEvaluationResult result =
        _policyEngine.evaluatePolicy(
            systemOperationContext,
            dataHubPolicyInfo,
            resolvedAuthorizedUserSpec,
            "EDIT_ENTITY_TAGS",
            Optional.of(resourceSpec),
            Collections.emptyList());
    assertFalse(result.isGranted());

    // Verify no network calls
    verify(_entityClient, times(0)).getV2(any(), any(), any(), any());
  }

  @Test
  public void testEvaluatePolicyResourceFilterSpecificResourceMatchDomainAndContainer()
      throws Exception {
    final DataHubPolicyInfo dataHubPolicyInfo = new DataHubPolicyInfo();
    dataHubPolicyInfo.setType(METADATA_POLICY_TYPE);
    dataHubPolicyInfo.setState(ACTIVE_POLICY_STATE);
    dataHubPolicyInfo.setPrivileges(new StringArray("EDIT_ENTITY_TAGS"));
    dataHubPolicyInfo.setDisplayName("My Test Display");
    dataHubPolicyInfo.setDescription("My test display!");
    dataHubPolicyInfo.setEditable(true);

    final DataHubActorFilter actorFilter = new DataHubActorFilter();
    actorFilter.setResourceOwners(true);
    actorFilter.setAllUsers(true);
    actorFilter.setAllGroups(true);
    dataHubPolicyInfo.setActors(actorFilter);

    final DataHubResourceFilter resourceFilter = new DataHubResourceFilter();
    resourceFilter.setFilter(
        FilterUtils.newFilter(
            ImmutableMap.of(
                EntityFieldType.TYPE,
                Collections.singletonList("dataset"),
                EntityFieldType.DOMAIN,
                Collections.singletonList(DOMAIN_URN),
                EntityFieldType.CONTAINER,
                Collections.singletonList(CONTAINER_URN))));
    dataHubPolicyInfo.setResources(resourceFilter);

    ResolvedEntitySpec resourceSpec =
        buildEntityResolvers(
            "dataset",
            RESOURCE_URN,
            Collections.emptySet(),
            Collections.singleton(DOMAIN_URN),
            Collections.singleton(CONTAINER_URN),
            Collections.emptySet(),
            Collections.emptySet());
    PolicyEngine.PolicyEvaluationResult result =
        _policyEngine.evaluatePolicy(
            systemOperationContext,
            dataHubPolicyInfo,
            resolvedAuthorizedUserSpec,
            "EDIT_ENTITY_TAGS",
            Optional.of(resourceSpec),
            Collections.emptyList());
    assertTrue(result.isGranted());

    // Verify no network calls
    verify(_entityClient, times(0)).getV2(any(), any(), any(), any());
  }

  @Test
  public void testEvaluatePolicyResourceFilterSpecificResourceMatchDomainNotContainer()
      throws Exception {
    final DataHubPolicyInfo dataHubPolicyInfo = new DataHubPolicyInfo();
    dataHubPolicyInfo.setType(METADATA_POLICY_TYPE);
    dataHubPolicyInfo.setState(ACTIVE_POLICY_STATE);
    dataHubPolicyInfo.setPrivileges(new StringArray("EDIT_ENTITY_TAGS"));
    dataHubPolicyInfo.setDisplayName("My Test Display");
    dataHubPolicyInfo.setDescription("My test display!");
    dataHubPolicyInfo.setEditable(true);

    final DataHubActorFilter actorFilter = new DataHubActorFilter();
    actorFilter.setResourceOwners(true);
    actorFilter.setAllUsers(true);
    actorFilter.setAllGroups(true);
    dataHubPolicyInfo.setActors(actorFilter);

    final DataHubResourceFilter resourceFilter = new DataHubResourceFilter();
    resourceFilter.setFilter(
        FilterUtils.newFilter(
            ImmutableMap.of(
                EntityFieldType.TYPE,
                Collections.singletonList("dataset"),
                EntityFieldType.DOMAIN,
                Collections.singletonList(DOMAIN_URN),
                EntityFieldType.CONTAINER,
                Collections.singletonList(CONTAINER_URN))));
    dataHubPolicyInfo.setResources(resourceFilter);

    ResolvedEntitySpec resourceSpec =
        buildEntityResolvers(
            "dataset",
            RESOURCE_URN,
            Collections.emptySet(),
            Collections.singleton(DOMAIN_URN),
            Collections.emptySet(),
            Collections.emptySet(),
            Collections.emptySet());
    PolicyEngine.PolicyEvaluationResult result =
        _policyEngine.evaluatePolicy(
            systemOperationContext,
            dataHubPolicyInfo,
            resolvedAuthorizedUserSpec,
            "EDIT_ENTITY_TAGS",
            Optional.of(resourceSpec),
            Collections.emptyList());
    assertFalse(result.isGranted());

    // Verify no network calls
    verify(_entityClient, times(0)).getV2(any(), any(), any(), any());
  }

  @Test
  public void testEvaluatePolicyResourceFilterSpecificResourceMatchContainerNotDomain()
      throws Exception {
    final DataHubPolicyInfo dataHubPolicyInfo = new DataHubPolicyInfo();
    dataHubPolicyInfo.setType(METADATA_POLICY_TYPE);
    dataHubPolicyInfo.setState(ACTIVE_POLICY_STATE);
    dataHubPolicyInfo.setPrivileges(new StringArray("EDIT_ENTITY_TAGS"));
    dataHubPolicyInfo.setDisplayName("My Test Display");
    dataHubPolicyInfo.setDescription("My test display!");
    dataHubPolicyInfo.setEditable(true);

    final DataHubActorFilter actorFilter = new DataHubActorFilter();
    actorFilter.setResourceOwners(true);
    actorFilter.setAllUsers(true);
    actorFilter.setAllGroups(true);
    dataHubPolicyInfo.setActors(actorFilter);

    final DataHubResourceFilter resourceFilter = new DataHubResourceFilter();
    resourceFilter.setFilter(
        FilterUtils.newFilter(
            ImmutableMap.of(
                EntityFieldType.TYPE,
                Collections.singletonList("dataset"),
                EntityFieldType.DOMAIN,
                Collections.singletonList(DOMAIN_URN),
                EntityFieldType.CONTAINER,
                Collections.singletonList(CONTAINER_URN))));
    dataHubPolicyInfo.setResources(resourceFilter);

    ResolvedEntitySpec resourceSpec =
        buildEntityResolvers(
            "dataset",
            RESOURCE_URN,
            Collections.emptySet(),
            Collections.emptySet(),
            Collections.singleton(CONTAINER_URN),
            Collections.emptySet(),
            Collections.emptySet());
    PolicyEngine.PolicyEvaluationResult result =
        _policyEngine.evaluatePolicy(
            systemOperationContext,
            dataHubPolicyInfo,
            resolvedAuthorizedUserSpec,
            "EDIT_ENTITY_TAGS",
            Optional.of(resourceSpec),
            Collections.emptyList());
    assertFalse(result.isGranted());

    // Verify no network calls
    verify(_entityClient, times(0)).getV2(any(), any(), any(), any());
  }

  @Test
  public void testEvaluatePolicyResourceFilterSpecificResourceMatchTag() throws Exception {
    final DataHubPolicyInfo dataHubPolicyInfo = new DataHubPolicyInfo();
    dataHubPolicyInfo.setType(METADATA_POLICY_TYPE);
    dataHubPolicyInfo.setState(ACTIVE_POLICY_STATE);
    dataHubPolicyInfo.setPrivileges(new StringArray("VIEW_ENTITY_PAGE"));
    dataHubPolicyInfo.setDisplayName("Tag-based policy");
    dataHubPolicyInfo.setDescription("Allow viewing entity pages based on tags");
    dataHubPolicyInfo.setEditable(true);

    final DataHubActorFilter actorFilter = new DataHubActorFilter();
    actorFilter.setResourceOwners(true);
    actorFilter.setAllUsers(true);
    actorFilter.setAllGroups(true);
    dataHubPolicyInfo.setActors(actorFilter);

    final DataHubResourceFilter resourceFilter = new DataHubResourceFilter();
    resourceFilter.setFilter(
        FilterUtils.newFilter(
            ImmutableMap.of(
                EntityFieldType.TYPE,
                Collections.singletonList("dataset"),
                EntityFieldType.TAG,
                Collections.singletonList(TAG_URN))));
    dataHubPolicyInfo.setResources(resourceFilter);

    ResolvedEntitySpec resourceSpec =
        buildEntityResolvers(
            "dataset",
            RESOURCE_URN,
            Collections.emptySet(),
            Collections.emptySet(),
            Collections.emptySet(),
            Collections.emptySet(),
            Collections.singleton(TAG_URN));
    PolicyEngine.PolicyEvaluationResult result =
        _policyEngine.evaluatePolicy(
            systemOperationContext,
            dataHubPolicyInfo,
            resolvedAuthorizedUserSpec,
            "VIEW_ENTITY_PAGE",
            Optional.of(resourceSpec),
            Collections.emptyList());
    assertTrue(result.isGranted());

    // Verify no network calls
    verify(_entityClient, times(0)).batchGetV2(any(), any(), any(), any());
  }

  @Test
  public void testGetGrantedPrivileges() throws Exception {
    // Policy 1, match dataset type and domain
    final DataHubPolicyInfo dataHubPolicyInfo1 = new DataHubPolicyInfo();
    dataHubPolicyInfo1.setType(METADATA_POLICY_TYPE);
    dataHubPolicyInfo1.setState(ACTIVE_POLICY_STATE);
    dataHubPolicyInfo1.setPrivileges(new StringArray("PRIVILEGE_1"));
    dataHubPolicyInfo1.setDisplayName("My Test Display");
    dataHubPolicyInfo1.setDescription("My test display!");
    dataHubPolicyInfo1.setEditable(true);

    final DataHubActorFilter actorFilter1 = new DataHubActorFilter();
    actorFilter1.setResourceOwners(true);
    actorFilter1.setAllUsers(true);
    actorFilter1.setAllGroups(true);
    dataHubPolicyInfo1.setActors(actorFilter1);

    final DataHubResourceFilter resourceFilter1 = new DataHubResourceFilter();
    resourceFilter1.setFilter(
        FilterUtils.newFilter(
            ImmutableMap.of(
                EntityFieldType.TYPE,
                Collections.singletonList("dataset"),
                EntityFieldType.DOMAIN,
                Collections.singletonList(DOMAIN_URN))));
    dataHubPolicyInfo1.setResources(resourceFilter1);

    // Policy 2, match dataset type and resource
    final DataHubPolicyInfo dataHubPolicyInfo2 = new DataHubPolicyInfo();
    dataHubPolicyInfo2.setType(METADATA_POLICY_TYPE);
    dataHubPolicyInfo2.setState(ACTIVE_POLICY_STATE);
    dataHubPolicyInfo2.setPrivileges(new StringArray("PRIVILEGE_2_1", "PRIVILEGE_2_2"));
    dataHubPolicyInfo2.setDisplayName("My Test Display");
    dataHubPolicyInfo2.setDescription("My test display!");
    dataHubPolicyInfo2.setEditable(true);

    final DataHubActorFilter actorFilter2 = new DataHubActorFilter();
    actorFilter2.setResourceOwners(true);
    actorFilter2.setAllUsers(true);
    actorFilter2.setAllGroups(true);
    dataHubPolicyInfo2.setActors(actorFilter2);

    final DataHubResourceFilter resourceFilter2 = new DataHubResourceFilter();
    resourceFilter2.setFilter(
        FilterUtils.newFilter(
            ImmutableMap.of(
                EntityFieldType.TYPE,
                Collections.singletonList("dataset"),
                EntityFieldType.URN,
                Collections.singletonList(RESOURCE_URN))));
    dataHubPolicyInfo2.setResources(resourceFilter2);

    // Policy 3, match dataset type and owner (legacy resource filter)
    final DataHubPolicyInfo dataHubPolicyInfo3 = new DataHubPolicyInfo();
    dataHubPolicyInfo3.setType(METADATA_POLICY_TYPE);
    dataHubPolicyInfo3.setState(ACTIVE_POLICY_STATE);
    dataHubPolicyInfo3.setPrivileges(new StringArray("PRIVILEGE_3"));
    dataHubPolicyInfo3.setDisplayName("My Test Display");
    dataHubPolicyInfo3.setDescription("My test display!");
    dataHubPolicyInfo3.setEditable(true);

    final DataHubActorFilter actorFilter3 = new DataHubActorFilter();
    actorFilter3.setResourceOwners(true);
    actorFilter3.setAllUsers(false);
    actorFilter3.setAllGroups(false);
    dataHubPolicyInfo3.setActors(actorFilter3);

    final DataHubResourceFilter resourceFilter3 = new DataHubResourceFilter();
    resourceFilter3.setAllResources(true);
    resourceFilter3.setType("dataset");
    dataHubPolicyInfo3.setResources(resourceFilter3);

    final List<DataHubPolicyInfo> policies =
        ImmutableList.of(dataHubPolicyInfo1, dataHubPolicyInfo2, dataHubPolicyInfo3);

    PolicyEngine.PolicyGrantedPrivileges grantedPrivileges =
        _policyEngine.getGrantedPrivileges(
            systemOperationContext,
            policies,
            resolvedAuthorizedUserSpec,
            Optional.empty(),
            Collections.emptyList());
    assertEquals(grantedPrivileges.getPrivileges(), Collections.emptyList());

    ResolvedEntitySpec resourceSpec =
        buildEntityResolvers(
            "dataset",
            RESOURCE_URN,
            Collections.emptySet(),
            Collections.singleton(DOMAIN_URN),
            Collections.emptySet(),
            Collections.emptySet(),
            Collections.emptySet()); // Everything matches

    grantedPrivileges =
        _policyEngine.getGrantedPrivileges(
            systemOperationContext,
            policies,
            resolvedAuthorizedUserSpec,
            Optional.of(resourceSpec),
            Collections.emptyList());
    assertEquals(
        new HashSet<>(grantedPrivileges.getPrivileges()),
        ImmutableSet.of("PRIVILEGE_1", "PRIVILEGE_2_1", "PRIVILEGE_2_2"));

    resourceSpec =
        buildEntityResolvers(
            "dataset",
            RESOURCE_URN,
            Collections.emptySet(),
            Collections.singleton("urn:li:domain:domain2"),
            Collections.emptySet(),
            Collections.emptySet(),
            Collections.emptySet()); // Domain doesn't match

    grantedPrivileges =
        _policyEngine.getGrantedPrivileges(
            systemOperationContext,
            policies,
            resolvedAuthorizedUserSpec,
            Optional.of(resourceSpec),
            Collections.emptyList());
    assertEquals(
        new HashSet<>(grantedPrivileges.getPrivileges()),
        ImmutableSet.of("PRIVILEGE_2_1", "PRIVILEGE_2_2"));

    resourceSpec =
        buildEntityResolvers(
            "dataset",
            "urn:li:dataset:random",
            Collections.emptySet(),
            Collections.singleton(DOMAIN_URN),
            Collections.emptySet(),
            Collections.emptySet(),
            Collections.emptySet()); // Resource doesn't match

    grantedPrivileges =
        _policyEngine.getGrantedPrivileges(
            systemOperationContext,
            policies,
            resolvedAuthorizedUserSpec,
            Optional.of(resourceSpec),
            Collections.emptyList());
    assertEquals(grantedPrivileges.getPrivileges(), ImmutableList.of("PRIVILEGE_1"));

    final EntityResponse entityResponse = new EntityResponse();
    final EnvelopedAspectMap aspectMap = new EnvelopedAspectMap();
    aspectMap.put(
        OWNERSHIP_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(createOwnershipAspect(true, false).data())));
    entityResponse.setAspects(aspectMap);
    when(_entityClient.getV2(
            eq(systemOperationContext),
            eq(resourceUrn.getEntityType()),
            eq(resourceUrn),
            eq(Collections.singleton(Constants.OWNERSHIP_ASPECT_NAME))))
        .thenReturn(entityResponse);
    resourceSpec =
        buildEntityResolvers(
            "dataset",
            RESOURCE_URN,
            Collections.singleton(AUTHORIZED_PRINCIPAL),
            Collections.singleton(DOMAIN_URN),
            Collections.emptySet(),
            Collections.emptySet(),
            Collections.emptySet()); // Is owner

    grantedPrivileges =
        _policyEngine.getGrantedPrivileges(
            systemOperationContext,
            policies,
            resolvedAuthorizedUserSpec,
            Optional.of(resourceSpec),
            Collections.emptyList());
    assertEquals(
        new HashSet<>(grantedPrivileges.getPrivileges()),
        ImmutableSet.of("PRIVILEGE_1", "PRIVILEGE_2_1", "PRIVILEGE_2_2", "PRIVILEGE_3"));

    resourceSpec =
        buildEntityResolvers(
            "chart",
            RESOURCE_URN,
            Collections.singleton(AUTHORIZED_PRINCIPAL),
            Collections.singleton(DOMAIN_URN),
            Collections.emptySet(),
            Collections.emptySet(),
            Collections.emptySet()); // Resource type doesn't match
    grantedPrivileges =
        _policyEngine.getGrantedPrivileges(
            systemOperationContext,
            policies,
            resolvedAuthorizedUserSpec,
            Optional.of(resourceSpec),
            Collections.emptyList());
    assertEquals(grantedPrivileges.getPrivileges(), Collections.emptyList());
  }

  @Test
  public void testGetMatchingActorsResourceMatch() throws Exception {

    final DataHubPolicyInfo dataHubPolicyInfo = new DataHubPolicyInfo();
    dataHubPolicyInfo.setType(METADATA_POLICY_TYPE);
    dataHubPolicyInfo.setState(ACTIVE_POLICY_STATE);
    dataHubPolicyInfo.setPrivileges(new StringArray("EDIT_ENTITY_TAGS"));
    dataHubPolicyInfo.setDisplayName("My Test Display");
    dataHubPolicyInfo.setDescription("My test display!");
    dataHubPolicyInfo.setEditable(true);

    final DataHubActorFilter actorFilter = new DataHubActorFilter();
    actorFilter.setResourceOwners(true);
    actorFilter.setAllUsers(true);
    actorFilter.setAllGroups(true);
    actorFilter.setUsers(
        new UrnArray(
            ImmutableList.of(
                Urn.createFromString("urn:li:corpuser:user1"),
                Urn.createFromString("urn:li:corpuser:user2"))));
    actorFilter.setGroups(
        new UrnArray(
            ImmutableList.of(
                Urn.createFromString("urn:li:corpGroup:group1"),
                Urn.createFromString("urn:li:corpGroup:group2"))));
    actorFilter.setRoles(new UrnArray(ImmutableList.of(Urn.createFromString("urn:li:role:Admin"))));
    dataHubPolicyInfo.setActors(actorFilter);

    final DataHubResourceFilter resourceFilter = new DataHubResourceFilter();
    resourceFilter.setAllResources(false);
    resourceFilter.setType("dataset");
    StringArray resourceUrns = new StringArray();
    resourceUrns.add(RESOURCE_URN); // Filter applies to specific resource.
    resourceFilter.setResources(resourceUrns);
    dataHubPolicyInfo.setResources(resourceFilter);

    ResolvedEntitySpec resourceSpec =
        buildEntityResolvers(
            "dataset",
            RESOURCE_URN,
            ImmutableSet.of(AUTHORIZED_PRINCIPAL, AUTHORIZED_GROUP),
            Collections.emptySet(),
            Collections.emptySet(),
            Collections.emptySet(),
            Collections.emptySet());
    PolicyEngine.PolicyActors actors =
        _policyEngine.getMatchingActors(dataHubPolicyInfo, Optional.of(resourceSpec));

    assertTrue(actors.getAllUsers());
    assertTrue(actors.getAllGroups());

    assertEquals(
        actors.getUsers(),
        ImmutableList.of(
            Urn.createFromString("urn:li:corpuser:user1"),
            Urn.createFromString("urn:li:corpuser:user2"),
            Urn.createFromString(AUTHORIZED_PRINCIPAL) // Resource Owner
            ));

    assertEquals(
        actors.getGroups(),
        ImmutableList.of(
            Urn.createFromString("urn:li:corpGroup:group1"),
            Urn.createFromString("urn:li:corpGroup:group2"),
            Urn.createFromString(AUTHORIZED_GROUP) // Resource Owner
            ));

    assertEquals(actors.getRoles(), ImmutableList.of(Urn.createFromString("urn:li:role:Admin")));

    // Verify aspect client called, entity client not called.
    verify(_entityClient, times(0))
        .batchGetV2(
            eq(systemOperationContext),
            eq(CORP_USER_ENTITY_NAME),
            eq(Collections.singleton(authorizedUserUrn)),
            eq(null));
  }

  @Test
  public void testGetMatchingActorsNoResourceMatch() throws Exception {

    final DataHubPolicyInfo dataHubPolicyInfo = new DataHubPolicyInfo();
    dataHubPolicyInfo.setType(METADATA_POLICY_TYPE);
    dataHubPolicyInfo.setState(ACTIVE_POLICY_STATE);
    dataHubPolicyInfo.setPrivileges(new StringArray("EDIT_ENTITY_TAGS"));
    dataHubPolicyInfo.setDisplayName("My Test Display");
    dataHubPolicyInfo.setDescription("My test display!");
    dataHubPolicyInfo.setEditable(true);

    final DataHubActorFilter actorFilter = new DataHubActorFilter();
    actorFilter.setResourceOwners(true);
    actorFilter.setAllUsers(true);
    actorFilter.setAllGroups(true);
    actorFilter.setUsers(
        new UrnArray(
            ImmutableList.of(
                Urn.createFromString("urn:li:corpuser:user1"),
                Urn.createFromString("urn:li:corpuser:user2"))));
    actorFilter.setGroups(
        new UrnArray(
            ImmutableList.of(
                Urn.createFromString("urn:li:corpGroup:group1"),
                Urn.createFromString("urn:li:corpGroup:group2"))));
    dataHubPolicyInfo.setActors(actorFilter);

    final DataHubResourceFilter resourceFilter = new DataHubResourceFilter();
    resourceFilter.setAllResources(false);
    resourceFilter.setType("dataset");
    StringArray resourceUrns = new StringArray();
    resourceUrns.add(RESOURCE_URN);
    resourceFilter.setResources(resourceUrns);
    dataHubPolicyInfo.setResources(resourceFilter);

    ResolvedEntitySpec resourceSpec =
        buildEntityResolvers(
            "dataset", "urn:li:dataset:random"); // A resource not covered by the policy.
    PolicyEngine.PolicyActors actors =
        _policyEngine.getMatchingActors(dataHubPolicyInfo, Optional.of(resourceSpec));

    assertFalse(actors.getAllUsers());
    assertFalse(actors.getAllGroups());
    assertEquals(actors.getUsers(), Collections.emptyList());
    assertEquals(actors.getGroups(), Collections.emptyList());
    // assertEquals(actors.getRoles(), Collections.emptyList());

    // Verify no network calls
    verify(_entityClient, times(0)).batchGetV2(any(), any(), any(), any());
  }

  @Test
  public void testGetMatchingActorsByRoleResourceMatch() throws Exception {
    final DataHubPolicyInfo dataHubPolicyInfo = new DataHubPolicyInfo();
    dataHubPolicyInfo.setType(METADATA_POLICY_TYPE);
    dataHubPolicyInfo.setState(ACTIVE_POLICY_STATE);
    dataHubPolicyInfo.setPrivileges(new StringArray("EDIT_ENTITY_TAGS"));
    dataHubPolicyInfo.setDisplayName("My Test Display");
    dataHubPolicyInfo.setDescription("My test display!");
    dataHubPolicyInfo.setEditable(true);

    final DataHubActorFilter actorFilter = new DataHubActorFilter();
    actorFilter.setResourceOwners(true);
    actorFilter.setAllUsers(false);
    actorFilter.setAllGroups(false);
    actorFilter.setRoles(
        new UrnArray(ImmutableList.of(Urn.createFromString("urn:li:dataHubRole:Editor"))));
    dataHubPolicyInfo.setActors(actorFilter);

    final DataHubResourceFilter resourceFilter = new DataHubResourceFilter();
    resourceFilter.setAllResources(false);
    resourceFilter.setType("dataset");
    StringArray resourceUrns = new StringArray();
    resourceUrns.add(RESOURCE_URN);
    resourceFilter.setResources(resourceUrns);
    dataHubPolicyInfo.setResources(resourceFilter);

    ResolvedEntitySpec resourceSpec =
        buildEntityResolvers(
            "dataset",
            RESOURCE_URN,
            ImmutableSet.of(),
            Collections.emptySet(),
            Collections.emptySet(),
            Collections.emptySet(),
            Collections.emptySet());

    PolicyEngine.PolicyActors actors =
        _policyEngine.getMatchingActors(dataHubPolicyInfo, Optional.of(resourceSpec));

    assertFalse(actors.getAllUsers());
    assertFalse(actors.getAllGroups());

    assertEquals(actors.getUsers(), ImmutableList.of());
    assertEquals(actors.getGroups(), ImmutableList.of());
    assertEquals(
        actors.getRoles(), ImmutableList.of(Urn.createFromString("urn:li:dataHubRole:Editor")));

    // Verify aspect client called, entity client not called.
    verify(_entityClient, times(0))
        .batchGetV2(
            eq(systemOperationContext),
            eq(CORP_USER_ENTITY_NAME),
            eq(Collections.singleton(authorizedUserUrn)),
            eq(null));
  }

  @Test
  public void testEvaluatePolicyWithSubResourceTagsAllowed() throws Exception {
    final DataHubPolicyInfo dataHubPolicyInfo = new DataHubPolicyInfo();
    dataHubPolicyInfo.setType(METADATA_POLICY_TYPE);
    dataHubPolicyInfo.setState(ACTIVE_POLICY_STATE);
    dataHubPolicyInfo.setPrivileges(new StringArray("EDIT_ENTITY_TAGS"));
    dataHubPolicyInfo.setDisplayName("Tag modification policy");
    dataHubPolicyInfo.setDescription("Policy that restricts which tags can be added/removed");
    dataHubPolicyInfo.setEditable(true);

    final DataHubActorFilter actorFilter = new DataHubActorFilter();
    actorFilter.setAllUsers(true);
    dataHubPolicyInfo.setActors(actorFilter);

    final DataHubResourceFilter resourceFilter = new DataHubResourceFilter();
    resourceFilter.setAllResources(true);
    resourceFilter.setType("dataset");

    // Set policy constraints - only allow modification of tags starting with "urn:li:tag:public"
    PolicyMatchCriterion tagCriterion =
        FilterUtils.newCriterion(
            EntityFieldType.URN,
            Collections.singletonList("urn:li:tag:public"),
            PolicyMatchCondition.STARTS_WITH);
    PolicyMatchFilter constraintFilter =
        new PolicyMatchFilter()
            .setCriteria(new PolicyMatchCriterionArray(Collections.singleton(tagCriterion)));
    resourceFilter.setPrivilegeConstraints(constraintFilter);

    dataHubPolicyInfo.setResources(resourceFilter);

    ResolvedEntitySpec resourceSpec = buildEntityResolvers("dataset", RESOURCE_URN);

    // Create sub-resources (tags being added) that match the constraint
    List<ResolvedEntitySpec> allowedTags =
        Arrays.asList(
            buildEntityResolvers("tag", "urn:li:tag:public_data"),
            buildEntityResolvers("tag", "urn:li:tag:public_analytics"));

    PolicyEngine.PolicyEvaluationResult result =
        _policyEngine.evaluatePolicy(
            systemOperationContext,
            dataHubPolicyInfo,
            resolvedAuthorizedUserSpec,
            "EDIT_ENTITY_TAGS",
            Optional.of(resourceSpec),
            allowedTags);

    assertTrue(result.isGranted());
  }

  @Test
  public void testEvaluatePolicyWithSubResourceTagsDenied() throws Exception {
    final DataHubPolicyInfo dataHubPolicyInfo = new DataHubPolicyInfo();
    dataHubPolicyInfo.setType(METADATA_POLICY_TYPE);
    dataHubPolicyInfo.setState(ACTIVE_POLICY_STATE);
    dataHubPolicyInfo.setPrivileges(new StringArray("EDIT_ENTITY_TAGS"));
    dataHubPolicyInfo.setDisplayName("Tag modification policy");
    dataHubPolicyInfo.setDescription("Policy that restricts which tags can be added/removed");
    dataHubPolicyInfo.setEditable(true);

    final DataHubActorFilter actorFilter = new DataHubActorFilter();
    actorFilter.setAllUsers(true);
    dataHubPolicyInfo.setActors(actorFilter);

    final DataHubResourceFilter resourceFilter = new DataHubResourceFilter();
    resourceFilter.setAllResources(true);
    resourceFilter.setType("dataset");

    // Set policy constraints - only allow modification of tags starting with "urn:li:tag:public"
    PolicyMatchCriterion tagCriterion =
        FilterUtils.newCriterion(
            EntityFieldType.URN,
            Collections.singletonList("urn:li:tag:public"),
            PolicyMatchCondition.STARTS_WITH);
    PolicyMatchFilter constraintFilter =
        new PolicyMatchFilter()
            .setCriteria(new PolicyMatchCriterionArray(Collections.singleton(tagCriterion)));
    resourceFilter.setPrivilegeConstraints(constraintFilter);

    dataHubPolicyInfo.setResources(resourceFilter);

    ResolvedEntitySpec resourceSpec = buildEntityResolvers("dataset", RESOURCE_URN);

    // Create sub-resources (tags being added) where some don't match the constraint
    List<ResolvedEntitySpec> mixedTags =
        Arrays.asList(
            buildEntityResolvers("tag", "urn:li:tag:public_data"), // Allowed
            buildEntityResolvers("tag", "urn:li:tag:sensitive_pii") // Not allowed
            );

    PolicyEngine.PolicyEvaluationResult result =
        _policyEngine.evaluatePolicy(
            systemOperationContext,
            dataHubPolicyInfo,
            resolvedAuthorizedUserSpec,
            "EDIT_ENTITY_TAGS",
            Optional.of(resourceSpec),
            mixedTags);

    assertFalse(result.isGranted());
  }

  @Test
  public void testEvaluatePolicyWithEmptySubResources() throws Exception {
    final DataHubPolicyInfo dataHubPolicyInfo = new DataHubPolicyInfo();
    dataHubPolicyInfo.setType(METADATA_POLICY_TYPE);
    dataHubPolicyInfo.setState(ACTIVE_POLICY_STATE);
    dataHubPolicyInfo.setPrivileges(new StringArray("EDIT_ENTITY_TAGS"));
    dataHubPolicyInfo.setDisplayName("Tag modification policy");
    dataHubPolicyInfo.setDescription("Policy that restricts which tags can be added/removed");
    dataHubPolicyInfo.setEditable(true);

    final DataHubActorFilter actorFilter = new DataHubActorFilter();
    actorFilter.setAllUsers(true);
    dataHubPolicyInfo.setActors(actorFilter);

    final DataHubResourceFilter resourceFilter = new DataHubResourceFilter();
    resourceFilter.setAllResources(true);
    resourceFilter.setType("dataset");

    // Set policy constraints for operation targets
    PolicyMatchCriterion tagCriterion =
        FilterUtils.newCriterion(
            EntityFieldType.URN,
            Collections.singletonList("urn:li:tag:public"),
            PolicyMatchCondition.STARTS_WITH);
    PolicyMatchFilter constraintFilter =
        new PolicyMatchFilter()
            .setCriteria(new PolicyMatchCriterionArray(Collections.singleton(tagCriterion)));
    resourceFilter.setPrivilegeConstraints(constraintFilter);

    dataHubPolicyInfo.setResources(resourceFilter);

    ResolvedEntitySpec resourceSpec = buildEntityResolvers("dataset", RESOURCE_URN);

    // Empty sub-resources (no tags being modified) should be allowed
    PolicyEngine.PolicyEvaluationResult result =
        _policyEngine.evaluatePolicy(
            systemOperationContext,
            dataHubPolicyInfo,
            resolvedAuthorizedUserSpec,
            "EDIT_ENTITY_TAGS",
            Optional.of(resourceSpec),
            Collections.emptyList());

    assertTrue(result.isGranted());
  }

  @Test
  public void testEvaluatePolicyWithNoPrivilegeConstraints() throws Exception {
    final DataHubPolicyInfo dataHubPolicyInfo = new DataHubPolicyInfo();
    dataHubPolicyInfo.setType(METADATA_POLICY_TYPE);
    dataHubPolicyInfo.setState(ACTIVE_POLICY_STATE);
    dataHubPolicyInfo.setPrivileges(new StringArray("EDIT_ENTITY_TAGS"));
    dataHubPolicyInfo.setDisplayName("Unrestricted tag policy");
    dataHubPolicyInfo.setDescription("Policy without operation target constraints");
    dataHubPolicyInfo.setEditable(true);

    final DataHubActorFilter actorFilter = new DataHubActorFilter();
    actorFilter.setAllUsers(true);
    dataHubPolicyInfo.setActors(actorFilter);

    final DataHubResourceFilter resourceFilter = new DataHubResourceFilter();
    resourceFilter.setAllResources(true);
    resourceFilter.setType("dataset");
    // No policy constraints set - any tags can be modified

    dataHubPolicyInfo.setResources(resourceFilter);

    ResolvedEntitySpec resourceSpec = buildEntityResolvers("dataset", RESOURCE_URN);

    // Any tags should be allowed when no constraints are defined
    List<ResolvedEntitySpec> anyTags =
        Arrays.asList(
            buildEntityResolvers("tag", "urn:li:tag:sensitive_pii"),
            buildEntityResolvers("tag", "urn:li:tag:public_data"));

    PolicyEngine.PolicyEvaluationResult result =
        _policyEngine.evaluatePolicy(
            systemOperationContext,
            dataHubPolicyInfo,
            resolvedAuthorizedUserSpec,
            "EDIT_ENTITY_TAGS",
            Optional.of(resourceSpec),
            anyTags);

    assertTrue(result.isGranted());
  }

  @Test
  public void testEvaluatePolicySubResourcesWithNotEqualsCondition() throws Exception {
    final DataHubPolicyInfo dataHubPolicyInfo = new DataHubPolicyInfo();
    dataHubPolicyInfo.setType(METADATA_POLICY_TYPE);
    dataHubPolicyInfo.setState(ACTIVE_POLICY_STATE);
    dataHubPolicyInfo.setPrivileges(new StringArray("EDIT_ENTITY_TAGS"));
    dataHubPolicyInfo.setDisplayName("Restricted tag policy with NOT_EQUALS");
    dataHubPolicyInfo.setDescription("Policy that prevents modification of specific tags");
    dataHubPolicyInfo.setEditable(true);

    final DataHubActorFilter actorFilter = new DataHubActorFilter();
    actorFilter.setAllUsers(true);
    dataHubPolicyInfo.setActors(actorFilter);

    final DataHubResourceFilter resourceFilter = new DataHubResourceFilter();
    resourceFilter.setAllResources(true);
    resourceFilter.setType("dataset");

    // Set policy constraints - exclude modification of specific restricted tags
    PolicyMatchCriterion tagCriterion =
        FilterUtils.newCriterion(
            EntityFieldType.URN,
            Collections.singletonList("urn:li:tag:restricted_pii"),
            PolicyMatchCondition.NOT_EQUALS);
    PolicyMatchFilter constraintFilter =
        new PolicyMatchFilter()
            .setCriteria(new PolicyMatchCriterionArray(Collections.singleton(tagCriterion)));
    resourceFilter.setPrivilegeConstraints(constraintFilter);

    dataHubPolicyInfo.setResources(resourceFilter);

    ResolvedEntitySpec resourceSpec = buildEntityResolvers("dataset", RESOURCE_URN);

    // Sub-resources (tags) that are allowed - not the restricted tag
    List<ResolvedEntitySpec> allowedTags =
        Arrays.asList(
            buildEntityResolvers("tag", "urn:li:tag:public_data"),
            buildEntityResolvers("tag", "urn:li:tag:analytics"));

    PolicyEngine.PolicyEvaluationResult result =
        _policyEngine.evaluatePolicy(
            systemOperationContext,
            dataHubPolicyInfo,
            resolvedAuthorizedUserSpec,
            "EDIT_ENTITY_TAGS",
            Optional.of(resourceSpec),
            allowedTags);

    assertTrue(result.isGranted());

    // Sub-resources that include the restricted tag should be denied
    List<ResolvedEntitySpec> restrictedTags =
        Arrays.asList(
            buildEntityResolvers("tag", "urn:li:tag:public_data"), // Allowed
            buildEntityResolvers("tag", "urn:li:tag:restricted_pii") // Not allowed
            );

    result =
        _policyEngine.evaluatePolicy(
            systemOperationContext,
            dataHubPolicyInfo,
            resolvedAuthorizedUserSpec,
            "EDIT_ENTITY_TAGS",
            Optional.of(resourceSpec),
            restrictedTags);

    assertFalse(result.isGranted());
  }

  @Test
  public void testEvaluatePolicySubResourcesWithStartsWithCondition() throws Exception {
    final DataHubPolicyInfo dataHubPolicyInfo = new DataHubPolicyInfo();
    dataHubPolicyInfo.setType(METADATA_POLICY_TYPE);
    dataHubPolicyInfo.setState(ACTIVE_POLICY_STATE);
    dataHubPolicyInfo.setPrivileges(new StringArray("EDIT_ENTITY_TAGS"));
    dataHubPolicyInfo.setDisplayName("Prefix-based tag policy with STARTS_WITH");
    dataHubPolicyInfo.setDescription(
        "Policy that only allows modification of tags with specific prefix");
    dataHubPolicyInfo.setEditable(true);

    final DataHubActorFilter actorFilter = new DataHubActorFilter();
    actorFilter.setAllUsers(true);
    dataHubPolicyInfo.setActors(actorFilter);

    final DataHubResourceFilter resourceFilter = new DataHubResourceFilter();
    resourceFilter.setAllResources(true);
    resourceFilter.setType("dataset");

    // Set policy constraints - URN must start with specific prefix for tags
    PolicyMatchCriterion urnCriterion =
        FilterUtils.newCriterion(
            EntityFieldType.URN,
            Collections.singletonList("urn:li:tag:department_"),
            PolicyMatchCondition.STARTS_WITH);
    PolicyMatchFilter constraintFilter =
        new PolicyMatchFilter()
            .setCriteria(new PolicyMatchCriterionArray(Collections.singleton(urnCriterion)));
    resourceFilter.setPrivilegeConstraints(constraintFilter);

    dataHubPolicyInfo.setResources(resourceFilter);

    ResolvedEntitySpec resourceSpec = buildEntityResolvers("dataset", RESOURCE_URN);

    // Sub-resources (tags) with allowed URN prefix
    List<ResolvedEntitySpec> allowedTags =
        Arrays.asList(
            buildEntityResolvers("tag", "urn:li:tag:department_finance"),
            buildEntityResolvers("tag", "urn:li:tag:department_engineering"));

    PolicyEngine.PolicyEvaluationResult result =
        _policyEngine.evaluatePolicy(
            systemOperationContext,
            dataHubPolicyInfo,
            resolvedAuthorizedUserSpec,
            "EDIT_ENTITY_TAGS",
            Optional.of(resourceSpec),
            allowedTags);

    assertTrue(result.isGranted());

    // Sub-resources (tags) with disallowed URN prefix should be denied
    List<ResolvedEntitySpec> disallowedTags =
        Arrays.asList(
            buildEntityResolvers("tag", "urn:li:tag:department_finance"), // Allowed
            buildEntityResolvers(
                "tag", "urn:li:tag:sensitive_pii") // Doesn't start with "department_"
            );

    result =
        _policyEngine.evaluatePolicy(
            systemOperationContext,
            dataHubPolicyInfo,
            resolvedAuthorizedUserSpec,
            "EDIT_ENTITY_TAGS",
            Optional.of(resourceSpec),
            disallowedTags);

    assertFalse(result.isGranted());
  }

  @Test
  public void testEvaluatePolicyOwnershipModificationConstraints() throws Exception {
    final DataHubPolicyInfo dataHubPolicyInfo = new DataHubPolicyInfo();
    dataHubPolicyInfo.setType(METADATA_POLICY_TYPE);
    dataHubPolicyInfo.setState(ACTIVE_POLICY_STATE);
    dataHubPolicyInfo.setPrivileges(new StringArray("EDIT_ENTITY_OWNERS"));
    dataHubPolicyInfo.setDisplayName("Ownership modification policy");
    dataHubPolicyInfo.setDescription("Policy that restricts which ownership types can be modified");
    dataHubPolicyInfo.setEditable(true);

    final DataHubActorFilter actorFilter = new DataHubActorFilter();
    actorFilter.setAllUsers(true);
    dataHubPolicyInfo.setActors(actorFilter);

    final DataHubResourceFilter resourceFilter = new DataHubResourceFilter();
    resourceFilter.setAllResources(true);
    resourceFilter.setType("dataset");

    // Set policy constraints - only allow modification of business ownership types
    PolicyMatchCriterion ownershipCriterion =
        FilterUtils.newCriterion(
            EntityFieldType.URN,
            Collections.singletonList("urn:li:ownershipType:business"),
            PolicyMatchCondition.STARTS_WITH);
    PolicyMatchFilter constraintFilter =
        new PolicyMatchFilter()
            .setCriteria(new PolicyMatchCriterionArray(Collections.singleton(ownershipCriterion)));
    resourceFilter.setPrivilegeConstraints(constraintFilter);

    dataHubPolicyInfo.setResources(resourceFilter);

    ResolvedEntitySpec resourceSpec = buildEntityResolvers("dataset", RESOURCE_URN);

    // Sub-resources (ownership assignments) that are allowed
    List<ResolvedEntitySpec> allowedOwnerships =
        Arrays.asList(
            buildEntityResolvers("ownershipType", "urn:li:ownershipType:business_owner"),
            buildEntityResolvers("ownershipType", "urn:li:ownershipType:business_steward"));

    PolicyEngine.PolicyEvaluationResult result =
        _policyEngine.evaluatePolicy(
            systemOperationContext,
            dataHubPolicyInfo,
            resolvedAuthorizedUserSpec,
            "EDIT_ENTITY_OWNERS",
            Optional.of(resourceSpec),
            allowedOwnerships);

    assertTrue(result.isGranted());

    // Sub-resources that include technical ownership types should be denied
    List<ResolvedEntitySpec> mixedOwnerships =
        Arrays.asList(
            buildEntityResolvers("ownershipType", "urn:li:ownershipType:business_owner"), // Allowed
            buildEntityResolvers(
                "ownershipType", "urn:li:ownershipType:technical_owner") // Not allowed
            );

    result =
        _policyEngine.evaluatePolicy(
            systemOperationContext,
            dataHubPolicyInfo,
            resolvedAuthorizedUserSpec,
            "EDIT_ENTITY_OWNERS",
            Optional.of(resourceSpec),
            mixedOwnerships);

    assertFalse(result.isGranted());
  }

  private Ownership createOwnershipAspect(final Boolean addUserOwner, final Boolean addGroupOwner)
      throws Exception {
    final Ownership ownershipAspect = new Ownership();
    final OwnerArray owners = new OwnerArray();

    if (addUserOwner) {
      final Owner userOwner = new Owner();
      userOwner.setOwner(Urn.createFromString(AUTHORIZED_PRINCIPAL));
      userOwner.setType(OwnershipType.DATAOWNER);
      owners.add(userOwner);
    }

    if (addGroupOwner) {
      final Owner groupOwner = new Owner();
      groupOwner.setOwner(Urn.createFromString(AUTHORIZED_GROUP));
      groupOwner.setType(OwnershipType.DATAOWNER);
      owners.add(groupOwner);
    }

    ownershipAspect.setOwners(owners);
    ownershipAspect.setLastModified(
        new AuditStamp().setTime(0).setActor(Urn.createFromString("urn:li:corpuser:foo")));
    return ownershipAspect;
  }

  private Ownership createOwnershipAspectWithTypeUrn(final String typeUrn) throws Exception {
    final Ownership ownershipAspect = new Ownership();
    final OwnerArray owners = new OwnerArray();

    final Owner userOwner = new Owner();
    userOwner.setOwner(Urn.createFromString(AUTHORIZED_PRINCIPAL));
    userOwner.setTypeUrn(Urn.createFromString(typeUrn));
    owners.add(userOwner);

    ownershipAspect.setOwners(owners);
    ownershipAspect.setLastModified(
        new AuditStamp().setTime(0).setActor(Urn.createFromString("urn:li:corpuser:foo")));
    return ownershipAspect;
  }

  private EntityResponse createAuthorizedEntityResponse() throws URISyntaxException {
    final EntityResponse entityResponse = new EntityResponse();
    final EnvelopedAspectMap aspectMap = new EnvelopedAspectMap();

    final RoleMembership rolesAspect = new RoleMembership();
    final UrnArray roles = new UrnArray();
    roles.add(Urn.createFromString("urn:li:dataHubRole:admin"));
    rolesAspect.setRoles(roles);
    aspectMap.put(
        ROLE_MEMBERSHIP_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(rolesAspect.data())));

    entityResponse.setAspects(aspectMap);
    return entityResponse;
  }

  private EntityResponse createUnauthorizedEntityResponse() throws URISyntaxException {
    final EntityResponse entityResponse = new EntityResponse();
    final EnvelopedAspectMap aspectMap = new EnvelopedAspectMap();

    final RoleMembership rolesAspect = new RoleMembership();
    final UrnArray roles = new UrnArray();
    roles.add(Urn.createFromString("urn:li:dataHubRole:reader"));
    rolesAspect.setRoles(roles);
    aspectMap.put(
        ROLE_MEMBERSHIP_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(rolesAspect.data())));

    entityResponse.setAspects(aspectMap);
    return entityResponse;
  }

  public static ResolvedEntitySpec buildEntityResolvers(String entityType, String entityUrn) {
    return buildEntityResolvers(
        entityType,
        entityUrn,
        Collections.emptySet(),
        Collections.emptySet(),
        Collections.emptySet(),
        Collections.emptySet(),
        Collections.emptySet());
  }

  public static ResolvedEntitySpec buildEntityResolvers(
      String entityType,
      String entityUrn,
      Set<String> owners,
      Set<String> domains,
      Set<String> containers,
      Set<String> groups,
      Set<String> tags) {
    return new ResolvedEntitySpec(
        new EntitySpec(entityType, entityUrn),
        ImmutableMap.of(
            EntityFieldType.TYPE,
            FieldResolver.getResolverFromValues(Collections.singleton(entityType)),
            EntityFieldType.URN,
            FieldResolver.getResolverFromValues(Collections.singleton(entityUrn)),
            EntityFieldType.OWNER,
            FieldResolver.getResolverFromValues(owners),
            EntityFieldType.DOMAIN,
            FieldResolver.getResolverFromValues(domains),
            EntityFieldType.CONTAINER,
            FieldResolver.getResolverFromValues(containers),
            EntityFieldType.GROUP_MEMBERSHIP,
            FieldResolver.getResolverFromValues(groups),
            EntityFieldType.TAG,
            FieldResolver.getResolverFromValues(tags)));
  }
}
