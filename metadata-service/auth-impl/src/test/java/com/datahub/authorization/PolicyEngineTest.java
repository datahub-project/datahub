package com.datahub.authorization;

import static com.linkedin.metadata.Constants.*;
import static com.linkedin.metadata.authorization.PoliciesConfig.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.datahub.authentication.Authentication;
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
import com.linkedin.policy.DataHubActorFilter;
import com.linkedin.policy.DataHubPolicyInfo;
import com.linkedin.policy.DataHubResourceFilter;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class PolicyEngineTest {

  private static final String AUTHORIZED_PRINCIPAL = "urn:li:corpuser:datahub";
  private static final String UNAUTHORIZED_PRINCIPAL = "urn:li:corpuser:unauthorized";
  private static final String AUTHORIZED_GROUP = "urn:li:corpGroup:authorizedGroup";
  private static final String RESOURCE_URN = "urn:li:dataset:test";
  private static final String DOMAIN_URN = "urn:li:domain:domain1";
  private static final String OWNERSHIP_TYPE_URN = "urn:li:ownershipType:__system__technical_owner";
  private static final String OTHER_OWNERSHIP_TYPE_URN =
      "urn:li:ownershipType:__system__data_steward";

  private EntityClient _entityClient;
  private PolicyEngine _policyEngine;

  private Urn authorizedUserUrn;
  private ResolvedEntitySpec resolvedAuthorizedUserSpec;
  private Urn unauthorizedUserUrn;
  private ResolvedEntitySpec resolvedUnauthorizedUserSpec;
  private Urn resourceUrn;

  @BeforeMethod
  public void setupTest() throws Exception {
    _entityClient = Mockito.mock(EntityClient.class);
    _policyEngine = new PolicyEngine(Mockito.mock(Authentication.class), _entityClient);

    authorizedUserUrn = Urn.createFromString(AUTHORIZED_PRINCIPAL);
    resolvedAuthorizedUserSpec =
        buildEntityResolvers(
            CORP_USER_ENTITY_NAME,
            AUTHORIZED_PRINCIPAL,
            Collections.emptySet(),
            Collections.emptySet(),
            Collections.singleton(AUTHORIZED_GROUP));
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
            eq(CORP_USER_ENTITY_NAME),
            eq(Collections.singleton(authorizedUserUrn)),
            eq(Collections.singleton(ROLE_MEMBERSHIP_ASPECT_NAME)),
            any()))
        .thenReturn(authorizedEntityResponseMap);

    EntityResponse unauthorizedEntityResponse = createUnauthorizedEntityResponse();
    unauthorizedEntityResponse.setUrn(unauthorizedUserUrn);
    Map<Urn, EntityResponse> unauthorizedEntityResponseMap =
        Collections.singletonMap(unauthorizedUserUrn, unauthorizedEntityResponse);
    when(_entityClient.batchGetV2(
            eq(CORP_USER_ENTITY_NAME),
            eq(Collections.singleton(unauthorizedUserUrn)),
            eq(Collections.singleton(ROLE_MEMBERSHIP_ASPECT_NAME)),
            any()))
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
            any(),
            eq(Collections.singleton(resourceUrn)),
            eq(Collections.singleton(OWNERSHIP_ASPECT_NAME)),
            any()))
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
            dataHubPolicyInfo,
            resolvedAuthorizedUserSpec,
            "EDIT_ENTITY_TAGS",
            Optional.of(resourceSpec));

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
            dataHubPolicyInfo,
            resolvedAuthorizedUserSpec,
            "EDIT_ENTITY_OWNERS",
            Optional.of(resourceSpec));
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
            dataHubPolicyInfo, resolvedAuthorizedUserSpec, "MANAGE_POLICIES", Optional.empty());
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
            dataHubPolicyInfo,
            resolvedAuthorizedUserSpec,
            "EDIT_ENTITY_TAGS",
            Optional.of(resourceSpec));

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
            dataHubPolicyInfo,
            buildEntityResolvers(CORP_USER_ENTITY_NAME, "urn:li:corpuser:test"),
            "EDIT_ENTITY_TAGS",
            Optional.of(resourceSpec));

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
            dataHubPolicyInfo,
            resolvedAuthorizedUserSpec,
            "EDIT_ENTITY_TAGS",
            Optional.of(resourceSpec));
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
            dataHubPolicyInfo,
            resolvedUnauthorizedUserSpec,
            "EDIT_ENTITY_TAGS",
            Optional.of(resourceSpec));
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
            dataHubPolicyInfo,
            resolvedAuthorizedUserSpec,
            "EDIT_ENTITY_TAGS",
            Optional.of(resourceSpec));

    assertTrue(authorizedResult.isGranted());

    // Verify we are only calling for roles during these requests.
    verify(_entityClient, times(1))
        .batchGetV2(
            eq(CORP_USER_ENTITY_NAME), eq(Collections.singleton(authorizedUserUrn)), any(), any());
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
            dataHubPolicyInfo,
            resolvedUnauthorizedUserSpec,
            "EDIT_ENTITY_TAGS",
            Optional.of(resourceSpec));

    assertFalse(unauthorizedResult.isGranted());

    // Verify we are only calling for roles during these requests.
    verify(_entityClient, times(1))
        .batchGetV2(
            eq(CORP_USER_ENTITY_NAME),
            eq(Collections.singleton(unauthorizedUserUrn)),
            any(),
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
            dataHubPolicyInfo,
            resolvedAuthorizedUserSpec,
            "EDIT_ENTITY_TAGS",
            Optional.of(resourceSpec));
    assertTrue(result1.isGranted());

    // Assert unauthorized user cannot edit entity tags.
    PolicyEngine.PolicyEvaluationResult result2 =
        _policyEngine.evaluatePolicy(
            dataHubPolicyInfo,
            resolvedUnauthorizedUserSpec,
            "EDIT_ENTITY_TAGS",
            Optional.of(resourceSpec));
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
            dataHubPolicyInfo,
            resolvedAuthorizedUserSpec,
            "EDIT_ENTITY_TAGS",
            Optional.of(resourceSpec));
    assertTrue(result1.isGranted());

    // Assert unauthorized user cannot edit entity tags.
    PolicyEngine.PolicyEvaluationResult result2 =
        _policyEngine.evaluatePolicy(
            dataHubPolicyInfo,
            resolvedUnauthorizedUserSpec,
            "EDIT_ENTITY_TAGS",
            Optional.of(resourceSpec));
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
            eq(resourceUrn.getEntityType()),
            eq(resourceUrn),
            eq(Collections.singleton(Constants.OWNERSHIP_ASPECT_NAME)),
            any()))
        .thenReturn(entityResponse);

    ResolvedEntitySpec resourceSpec =
        buildEntityResolvers(
            "dataset",
            RESOURCE_URN,
            ImmutableSet.of(AUTHORIZED_PRINCIPAL),
            Collections.emptySet(),
            Collections.emptySet());
    // Assert authorized user can edit entity tags, because he is a user owner.
    PolicyEngine.PolicyEvaluationResult result1 =
        _policyEngine.evaluatePolicy(
            dataHubPolicyInfo,
            resolvedAuthorizedUserSpec,
            "EDIT_ENTITY_TAGS",
            Optional.of(resourceSpec));
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
            eq(resourceUrn.getEntityType()),
            eq(resourceUrn),
            eq(Collections.singleton(Constants.OWNERSHIP_ASPECT_NAME)),
            any()))
        .thenReturn(entityResponse);

    ResolvedEntitySpec resourceSpec =
        buildEntityResolvers(
            "dataset",
            RESOURCE_URN,
            ImmutableSet.of(AUTHORIZED_PRINCIPAL),
            Collections.emptySet(),
            Collections.emptySet());

    PolicyEngine.PolicyEvaluationResult result1 =
        _policyEngine.evaluatePolicy(
            dataHubPolicyInfo,
            resolvedAuthorizedUserSpec,
            "EDIT_ENTITY_TAGS",
            Optional.of(resourceSpec));
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
            eq(resourceUrn.getEntityType()),
            eq(resourceUrn),
            eq(Collections.singleton(Constants.OWNERSHIP_ASPECT_NAME)),
            any()))
        .thenReturn(entityResponse);

    ResolvedEntitySpec resourceSpec =
        buildEntityResolvers(
            "dataset",
            RESOURCE_URN,
            ImmutableSet.of(AUTHORIZED_PRINCIPAL),
            Collections.emptySet(),
            Collections.emptySet());

    PolicyEngine.PolicyEvaluationResult result1 =
        _policyEngine.evaluatePolicy(
            dataHubPolicyInfo,
            resolvedAuthorizedUserSpec,
            "EDIT_ENTITY_TAGS",
            Optional.of(resourceSpec));
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
            eq(resourceUrn.getEntityType()),
            eq(resourceUrn),
            eq(Collections.singleton(Constants.OWNERSHIP_ASPECT_NAME)),
            any()))
        .thenReturn(entityResponse);

    ResolvedEntitySpec resourceSpec =
        buildEntityResolvers(
            "dataset",
            RESOURCE_URN,
            ImmutableSet.of(AUTHORIZED_GROUP),
            Collections.emptySet(),
            Collections.emptySet());
    // Assert authorized user can edit entity tags, because he is a user owner.
    PolicyEngine.PolicyEvaluationResult result1 =
        _policyEngine.evaluatePolicy(
            dataHubPolicyInfo,
            resolvedAuthorizedUserSpec,
            "EDIT_ENTITY_TAGS",
            Optional.of(resourceSpec));
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
            dataHubPolicyInfo,
            resolvedUnauthorizedUserSpec,
            "EDIT_ENTITY_TAGS",
            Optional.of(resourceSpec));
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
            dataHubPolicyInfo,
            resolvedAuthorizedUserSpec,
            "EDIT_ENTITY_TAGS",
            Optional.of(resourceSpec));
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
            dataHubPolicyInfo,
            resolvedAuthorizedUserSpec,
            "EDIT_ENTITY_TAGS",
            Optional.of(resourceSpec));
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
            dataHubPolicyInfo,
            resolvedAuthorizedUserSpec,
            "EDIT_ENTITY_TAGS",
            Optional.of(resourceSpec));
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
            dataHubPolicyInfo,
            resolvedAuthorizedUserSpec,
            "EDIT_ENTITY_TAGS",
            Optional.of(resourceSpec));
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
            dataHubPolicyInfo,
            resolvedAuthorizedUserSpec,
            "EDIT_ENTITY_TAGS",
            Optional.of(resourceSpec));
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
            Collections.emptySet());
    PolicyEngine.PolicyEvaluationResult result =
        _policyEngine.evaluatePolicy(
            dataHubPolicyInfo,
            resolvedAuthorizedUserSpec,
            "EDIT_ENTITY_TAGS",
            Optional.of(resourceSpec));
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
            Collections.emptySet()); // Domain doesn't match
    PolicyEngine.PolicyEvaluationResult result =
        _policyEngine.evaluatePolicy(
            dataHubPolicyInfo,
            resolvedAuthorizedUserSpec,
            "EDIT_ENTITY_TAGS",
            Optional.of(resourceSpec));
    assertFalse(result.isGranted());

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

    assertEquals(
        _policyEngine.getGrantedPrivileges(policies, resolvedAuthorizedUserSpec, Optional.empty()),
        Collections.emptyList());

    ResolvedEntitySpec resourceSpec =
        buildEntityResolvers(
            "dataset",
            RESOURCE_URN,
            Collections.emptySet(),
            Collections.singleton(DOMAIN_URN),
            Collections.emptySet()); // Everything matches
    assertEquals(
        _policyEngine.getGrantedPrivileges(
            policies, resolvedAuthorizedUserSpec, Optional.of(resourceSpec)),
        ImmutableList.of("PRIVILEGE_1", "PRIVILEGE_2_1", "PRIVILEGE_2_2"));

    resourceSpec =
        buildEntityResolvers(
            "dataset",
            RESOURCE_URN,
            Collections.emptySet(),
            Collections.singleton("urn:li:domain:domain2"),
            Collections.emptySet()); // Domain doesn't match
    assertEquals(
        _policyEngine.getGrantedPrivileges(
            policies, resolvedAuthorizedUserSpec, Optional.of(resourceSpec)),
        ImmutableList.of("PRIVILEGE_2_1", "PRIVILEGE_2_2"));

    resourceSpec =
        buildEntityResolvers(
            "dataset",
            "urn:li:dataset:random",
            Collections.emptySet(),
            Collections.singleton(DOMAIN_URN),
            Collections.emptySet()); // Resource doesn't match
    assertEquals(
        _policyEngine.getGrantedPrivileges(
            policies, resolvedAuthorizedUserSpec, Optional.of(resourceSpec)),
        ImmutableList.of("PRIVILEGE_1"));

    final EntityResponse entityResponse = new EntityResponse();
    final EnvelopedAspectMap aspectMap = new EnvelopedAspectMap();
    aspectMap.put(
        OWNERSHIP_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(createOwnershipAspect(true, false).data())));
    entityResponse.setAspects(aspectMap);
    when(_entityClient.getV2(
            eq(resourceUrn.getEntityType()),
            eq(resourceUrn),
            eq(Collections.singleton(Constants.OWNERSHIP_ASPECT_NAME)),
            any()))
        .thenReturn(entityResponse);
    resourceSpec =
        buildEntityResolvers(
            "dataset",
            RESOURCE_URN,
            Collections.singleton(AUTHORIZED_PRINCIPAL),
            Collections.singleton(DOMAIN_URN),
            Collections.emptySet()); // Is owner
    assertEquals(
        _policyEngine.getGrantedPrivileges(
            policies, resolvedAuthorizedUserSpec, Optional.of(resourceSpec)),
        ImmutableList.of("PRIVILEGE_1", "PRIVILEGE_2_1", "PRIVILEGE_2_2", "PRIVILEGE_3"));

    resourceSpec =
        buildEntityResolvers(
            "chart",
            RESOURCE_URN,
            Collections.singleton(AUTHORIZED_PRINCIPAL),
            Collections.singleton(DOMAIN_URN),
            Collections.emptySet()); // Resource type doesn't match
    assertEquals(
        _policyEngine.getGrantedPrivileges(
            policies, resolvedAuthorizedUserSpec, Optional.of(resourceSpec)),
        Collections.emptyList());
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
            eq(CORP_USER_ENTITY_NAME),
            eq(Collections.singleton(authorizedUserUrn)),
            eq(null),
            any());
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
            eq(CORP_USER_ENTITY_NAME),
            eq(Collections.singleton(authorizedUserUrn)),
            eq(null),
            any());
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
        Collections.emptySet());
  }

  public static ResolvedEntitySpec buildEntityResolvers(
      String entityType,
      String entityUrn,
      Set<String> owners,
      Set<String> domains,
      Set<String> groups) {
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
            EntityFieldType.GROUP_MEMBERSHIP,
            FieldResolver.getResolverFromValues(groups)));
  }
}
