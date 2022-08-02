package com.datahub.authorization;

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
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringArray;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.identity.CorpUserInfo;
import com.linkedin.identity.GroupMembership;
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

import static com.linkedin.metadata.Constants.*;
import static com.linkedin.metadata.authorization.PoliciesConfig.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;


public class PolicyEngineTest {

  private static final String AUTHORIZED_PRINCIPAL = "urn:li:corpuser:datahub";
  private static final String UNAUTHORIZED_PRINCIPAL = "urn:li:corpuser:unauthorized";

  private static final String AUTHORIZED_GROUP = "urn:li:corpGroup:authorizedGroup";

  private static final String RESOURCE_URN = "urn:li:dataset:test";

  private static final String DOMAIN_URN = "urn:li:domain:domain1";

  private EntityClient _entityClient;
  private PolicyEngine _policyEngine;

  private Urn authorizedUserUrn;
  private Urn unauthorizedUserUrn;
  private Urn resourceUrn;

  @BeforeMethod
  public void setupTest() throws Exception {
    _entityClient = Mockito.mock(EntityClient.class);
    _policyEngine = new PolicyEngine(Mockito.mock(Authentication.class), _entityClient);

    // Init mocks.
    EntityResponse authorizedEntityResponse = createAuthorizedEntityResponse();
    authorizedUserUrn = Urn.createFromString(AUTHORIZED_PRINCIPAL);
    authorizedEntityResponse.setUrn(authorizedUserUrn);
    Map<Urn, EntityResponse> authorizedEntityResponseMap =
        Collections.singletonMap(authorizedUserUrn, authorizedEntityResponse);
    when(_entityClient.batchGetV2(eq(CORP_USER_ENTITY_NAME), eq(Collections.singleton(authorizedUserUrn)), eq(null),
        any())).thenReturn(authorizedEntityResponseMap);

    EntityResponse unauthorizedEntityResponse = createUnauthorizedEntityResponse();
    unauthorizedUserUrn = Urn.createFromString(UNAUTHORIZED_PRINCIPAL);
    unauthorizedEntityResponse.setUrn(unauthorizedUserUrn);
    Map<Urn, EntityResponse> unauthorizedEntityResponseMap =
        Collections.singletonMap(unauthorizedUserUrn, unauthorizedEntityResponse);
    when(_entityClient.batchGetV2(eq(CORP_USER_ENTITY_NAME), eq(Collections.singleton(unauthorizedUserUrn)), eq(null),
        any())).thenReturn(unauthorizedEntityResponseMap);

    EntityResponse entityResponse = new EntityResponse();
    EnvelopedAspectMap envelopedAspectMap = new EnvelopedAspectMap();
    envelopedAspectMap.put(OWNERSHIP_ASPECT_NAME,
        new EnvelopedAspect().setValue(new com.linkedin.entity.Aspect(createOwnershipAspect(true, true).data())));
    entityResponse.setAspects(envelopedAspectMap);
    resourceUrn = Urn.createFromString(RESOURCE_URN);
    Map<Urn, EntityResponse> mockMap = mock(Map.class);
    when(_entityClient.batchGetV2(any(), eq(Collections.singleton(resourceUrn)),
        eq(Collections.singleton(OWNERSHIP_ASPECT_NAME)), any())).thenReturn(mockMap);
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
    ResolvedResourceSpec resourceSpec = buildResourceResolvers("dataset", RESOURCE_URN);
    PolicyEngine.PolicyEvaluationResult result =
        _policyEngine.evaluatePolicy(dataHubPolicyInfo, AUTHORIZED_PRINCIPAL, "EDIT_ENTITY_TAGS",
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

    ResolvedResourceSpec resourceSpec = buildResourceResolvers("dataset", RESOURCE_URN);
    PolicyEngine.PolicyEvaluationResult result =
        _policyEngine.evaluatePolicy(dataHubPolicyInfo, AUTHORIZED_PRINCIPAL, "EDIT_ENTITY_OWNERS",
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
        _policyEngine.evaluatePolicy(dataHubPolicyInfo, AUTHORIZED_PRINCIPAL, "MANAGE_POLICIES", Optional.empty());
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

    ResolvedResourceSpec resourceSpec = buildResourceResolvers("dataset", RESOURCE_URN);
    // Assert Authorized user can edit entity tags.
    PolicyEngine.PolicyEvaluationResult result1 =
        _policyEngine.evaluatePolicy(dataHubPolicyInfo, AUTHORIZED_PRINCIPAL, "EDIT_ENTITY_TAGS",
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

    ResolvedResourceSpec resourceSpec = buildResourceResolvers("dataset", RESOURCE_URN);
    // Assert unauthorized user cannot edit entity tags.
    PolicyEngine.PolicyEvaluationResult result2 =
        _policyEngine.evaluatePolicy(dataHubPolicyInfo, "urn:li:corpuser:test", "EDIT_ENTITY_TAGS",
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
    groupsUrnArray.add(Urn.createFromString("urn:li:corpGroup:authorizedGroup"));
    actorFilter.setGroups(groupsUrnArray);
    actorFilter.setResourceOwners(false);
    actorFilter.setAllUsers(false);
    actorFilter.setAllGroups(false);
    dataHubPolicyInfo.setActors(actorFilter);

    final DataHubResourceFilter resourceFilter = new DataHubResourceFilter();
    resourceFilter.setAllResources(true);
    resourceFilter.setType("dataset");
    dataHubPolicyInfo.setResources(resourceFilter);

    ResolvedResourceSpec resourceSpec = buildResourceResolvers("dataset", RESOURCE_URN);
    // Assert authorized user can edit entity tags, because of group membership.
    PolicyEngine.PolicyEvaluationResult result1 =
        _policyEngine.evaluatePolicy(dataHubPolicyInfo, AUTHORIZED_PRINCIPAL, "EDIT_ENTITY_TAGS",
            Optional.of(resourceSpec));
    assertTrue(result1.isGranted());

    // Verify we are only calling for group during these requests.
    verify(_entityClient, times(2)).batchGetV2(eq(CORP_USER_ENTITY_NAME), eq(Collections.singleton(authorizedUserUrn)),
        eq(null), any());
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
    groupsUrnArray.add(Urn.createFromString("urn:li:corpGroup:authorizedGroup"));
    actorFilter.setGroups(groupsUrnArray);
    actorFilter.setResourceOwners(false);
    actorFilter.setAllUsers(false);
    actorFilter.setAllGroups(false);
    dataHubPolicyInfo.setActors(actorFilter);

    final DataHubResourceFilter resourceFilter = new DataHubResourceFilter();
    resourceFilter.setAllResources(true);
    resourceFilter.setType("dataset");
    dataHubPolicyInfo.setResources(resourceFilter);

    ResolvedResourceSpec resourceSpec = buildResourceResolvers("dataset", RESOURCE_URN);
    // Assert unauthorized user cannot edit entity tags.
    PolicyEngine.PolicyEvaluationResult result2 =
        _policyEngine.evaluatePolicy(dataHubPolicyInfo, UNAUTHORIZED_PRINCIPAL, "EDIT_ENTITY_TAGS",
            Optional.of(resourceSpec));
    assertFalse(result2.isGranted());

    // Verify we are only calling for group during these requests.
    verify(_entityClient, times(2)).batchGetV2(eq(CORP_USER_ENTITY_NAME),
        eq(Collections.singleton(unauthorizedUserUrn)), eq(null), any());
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

    ResolvedResourceSpec resourceSpec = buildResourceResolvers("dataset", RESOURCE_URN);
    // Assert authorized user can edit entity tags, because of group membership.
    PolicyEngine.PolicyEvaluationResult result1 =
        _policyEngine.evaluatePolicy(dataHubPolicyInfo, AUTHORIZED_PRINCIPAL, "EDIT_ENTITY_TAGS",
            Optional.of(resourceSpec));
    assertTrue(result1.isGranted());

    // Assert unauthorized user cannot edit entity tags.
    PolicyEngine.PolicyEvaluationResult result2 =
        _policyEngine.evaluatePolicy(dataHubPolicyInfo, UNAUTHORIZED_PRINCIPAL, "EDIT_ENTITY_TAGS",
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

    ResolvedResourceSpec resourceSpec = buildResourceResolvers("dataset", RESOURCE_URN);
    // Assert authorized user can edit entity tags, because of group membership.
    PolicyEngine.PolicyEvaluationResult result1 =
        _policyEngine.evaluatePolicy(dataHubPolicyInfo, AUTHORIZED_PRINCIPAL, "EDIT_ENTITY_TAGS",
            Optional.of(resourceSpec));
    assertTrue(result1.isGranted());

    // Assert unauthorized user cannot edit entity tags.
    PolicyEngine.PolicyEvaluationResult result2 =
        _policyEngine.evaluatePolicy(dataHubPolicyInfo, UNAUTHORIZED_PRINCIPAL, "EDIT_ENTITY_TAGS",
            Optional.of(resourceSpec));
    assertTrue(result2.isGranted());

    // Verify we are only calling for group during these requests.
    verify(_entityClient, times(2)).batchGetV2(eq(CORP_USER_ENTITY_NAME), eq(Collections.singleton(authorizedUserUrn)),
        eq(null), any());
    verify(_entityClient, times(2)).batchGetV2(eq(CORP_USER_ENTITY_NAME),
        eq(Collections.singleton(unauthorizedUserUrn)), eq(null), any());
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

    ResolvedResourceSpec resourceSpec =
        buildResourceResolvers("dataset", RESOURCE_URN, ImmutableSet.of(AUTHORIZED_PRINCIPAL), Collections.emptySet());
    // Assert authorized user can edit entity tags, because he is a user owner.
    PolicyEngine.PolicyEvaluationResult result1 =
        _policyEngine.evaluatePolicy(dataHubPolicyInfo, AUTHORIZED_PRINCIPAL, "EDIT_ENTITY_TAGS",
            Optional.of(resourceSpec));
    assertTrue(result1.isGranted());

    // Ensure no calls for group membership.
    verify(_entityClient, times(0)).batchGetV2(eq(CORP_USER_ENTITY_NAME), eq(Collections.singleton(authorizedUserUrn)),
        eq(null), any());
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

    ResolvedResourceSpec resourceSpec =
        buildResourceResolvers("dataset", RESOURCE_URN, ImmutableSet.of(AUTHORIZED_GROUP), Collections.emptySet());
    // Assert authorized user can edit entity tags, because he is a user owner.
    PolicyEngine.PolicyEvaluationResult result1 =
        _policyEngine.evaluatePolicy(dataHubPolicyInfo, AUTHORIZED_PRINCIPAL, "EDIT_ENTITY_TAGS",
            Optional.of(resourceSpec));
    assertTrue(result1.isGranted());

    // Ensure that caching of groups is working with 2 calls (for groups and native groups) to entity client for each
    // principal.
    verify(_entityClient, times(2)).batchGetV2(eq(CORP_USER_ENTITY_NAME), eq(Collections.singleton(authorizedUserUrn)),
        eq(null), any());
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

    ResolvedResourceSpec resourceSpec = buildResourceResolvers("dataset", RESOURCE_URN);
    // Assert unauthorized user cannot edit entity tags.
    PolicyEngine.PolicyEvaluationResult result2 =
        _policyEngine.evaluatePolicy(dataHubPolicyInfo, UNAUTHORIZED_PRINCIPAL, "EDIT_ENTITY_TAGS",
            Optional.of(resourceSpec));
    assertFalse(result2.isGranted());

    // Ensure that caching of groups is working with 2 calls (for groups and native groups) to entity client for each
    // principal.
    verify(_entityClient, times(2)).batchGetV2(eq(CORP_USER_ENTITY_NAME),
        eq(Collections.singleton(unauthorizedUserUrn)), eq(null), any());
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

    ResolvedResourceSpec resourceSpec =
        buildResourceResolvers("dataset", "urn:li:dataset:random"); // A dataset Authorized principal _does not own_.
    PolicyEngine.PolicyEvaluationResult result =
        _policyEngine.evaluatePolicy(dataHubPolicyInfo, AUTHORIZED_PRINCIPAL, "EDIT_ENTITY_TAGS",
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

    ResolvedResourceSpec resourceSpec = buildResourceResolvers("chart", RESOURCE_URN); // Notice: Not a dataset.
    PolicyEngine.PolicyEvaluationResult result =
        _policyEngine.evaluatePolicy(dataHubPolicyInfo, AUTHORIZED_PRINCIPAL, "EDIT_ENTITY_TAGS",
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

    ResolvedResourceSpec resourceSpec = buildResourceResolvers("dataset", RESOURCE_URN);
    PolicyEngine.PolicyEvaluationResult result =
        _policyEngine.evaluatePolicy(dataHubPolicyInfo, AUTHORIZED_PRINCIPAL, "EDIT_ENTITY_TAGS",
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
    resourceFilter.setFilter(FilterUtils.newFilter(
        ImmutableMap.of(ResourceFieldType.RESOURCE_TYPE, Collections.singletonList("dataset"), ResourceFieldType.RESOURCE_URN,
            Collections.singletonList(RESOURCE_URN))));
    dataHubPolicyInfo.setResources(resourceFilter);

    ResolvedResourceSpec resourceSpec = buildResourceResolvers("dataset", RESOURCE_URN);
    PolicyEngine.PolicyEvaluationResult result =
        _policyEngine.evaluatePolicy(dataHubPolicyInfo, AUTHORIZED_PRINCIPAL, "EDIT_ENTITY_TAGS",
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
    resourceFilter.setFilter(FilterUtils.newFilter(
        ImmutableMap.of(ResourceFieldType.RESOURCE_TYPE, Collections.singletonList("dataset"), ResourceFieldType.RESOURCE_URN,
            Collections.singletonList(RESOURCE_URN))));
    dataHubPolicyInfo.setResources(resourceFilter);

    ResolvedResourceSpec resourceSpec =
        buildResourceResolvers("dataset", "urn:li:dataset:random"); // A resource not covered by the policy.
    PolicyEngine.PolicyEvaluationResult result =
        _policyEngine.evaluatePolicy(dataHubPolicyInfo, AUTHORIZED_PRINCIPAL, "EDIT_ENTITY_TAGS",
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
    resourceFilter.setFilter(FilterUtils.newFilter(
        ImmutableMap.of(ResourceFieldType.RESOURCE_TYPE, Collections.singletonList("dataset"), ResourceFieldType.DOMAIN,
            Collections.singletonList(DOMAIN_URN))));
    dataHubPolicyInfo.setResources(resourceFilter);

    ResolvedResourceSpec resourceSpec =
        buildResourceResolvers("dataset", RESOURCE_URN, Collections.emptySet(), Collections.singleton(DOMAIN_URN));
    PolicyEngine.PolicyEvaluationResult result =
        _policyEngine.evaluatePolicy(dataHubPolicyInfo, AUTHORIZED_PRINCIPAL, "EDIT_ENTITY_TAGS",
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
    resourceFilter.setFilter(FilterUtils.newFilter(
        ImmutableMap.of(ResourceFieldType.RESOURCE_TYPE, Collections.singletonList("dataset"), ResourceFieldType.DOMAIN,
            Collections.singletonList(DOMAIN_URN))));
    dataHubPolicyInfo.setResources(resourceFilter);

    ResolvedResourceSpec resourceSpec = buildResourceResolvers("dataset", RESOURCE_URN, Collections.emptySet(),
        Collections.singleton("urn:li:domain:domain2")); // Domain doesn't match
    PolicyEngine.PolicyEvaluationResult result =
        _policyEngine.evaluatePolicy(dataHubPolicyInfo, AUTHORIZED_PRINCIPAL, "EDIT_ENTITY_TAGS",
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
    resourceFilter1.setFilter(FilterUtils.newFilter(
        ImmutableMap.of(ResourceFieldType.RESOURCE_TYPE, Collections.singletonList("dataset"), ResourceFieldType.DOMAIN,
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
    resourceFilter2.setFilter(FilterUtils.newFilter(
        ImmutableMap.of(ResourceFieldType.RESOURCE_TYPE, Collections.singletonList("dataset"), ResourceFieldType.RESOURCE_URN,
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

    assertEquals(_policyEngine.getGrantedPrivileges(policies, UrnUtils.getUrn(AUTHORIZED_PRINCIPAL), Optional.empty()),
        Collections.emptyList());

    ResolvedResourceSpec resourceSpec = buildResourceResolvers("dataset", RESOURCE_URN, Collections.emptySet(),
        Collections.singleton(DOMAIN_URN)); // Everything matches
    assertEquals(
        _policyEngine.getGrantedPrivileges(policies, UrnUtils.getUrn(AUTHORIZED_PRINCIPAL), Optional.of(resourceSpec)),
        ImmutableList.of("PRIVILEGE_1", "PRIVILEGE_2_1", "PRIVILEGE_2_2"));

    resourceSpec = buildResourceResolvers("dataset", RESOURCE_URN, Collections.emptySet(),
        Collections.singleton("urn:li:domain:domain2")); // Domain doesn't match
    assertEquals(
        _policyEngine.getGrantedPrivileges(policies, UrnUtils.getUrn(AUTHORIZED_PRINCIPAL), Optional.of(resourceSpec)),
        ImmutableList.of("PRIVILEGE_2_1", "PRIVILEGE_2_2"));

    resourceSpec = buildResourceResolvers("dataset", "urn:li:dataset:random", Collections.emptySet(),
        Collections.singleton(DOMAIN_URN)); // Resource doesn't match
    assertEquals(
        _policyEngine.getGrantedPrivileges(policies, UrnUtils.getUrn(AUTHORIZED_PRINCIPAL), Optional.of(resourceSpec)),
        ImmutableList.of("PRIVILEGE_1"));

    resourceSpec = buildResourceResolvers("dataset", RESOURCE_URN, Collections.singleton(AUTHORIZED_PRINCIPAL),
        Collections.singleton(DOMAIN_URN)); // Is owner
    assertEquals(
        _policyEngine.getGrantedPrivileges(policies, UrnUtils.getUrn(AUTHORIZED_PRINCIPAL), Optional.of(resourceSpec)),
        ImmutableList.of("PRIVILEGE_1", "PRIVILEGE_2_1", "PRIVILEGE_2_2", "PRIVILEGE_3"));

    resourceSpec = buildResourceResolvers("chart", RESOURCE_URN, Collections.singleton(AUTHORIZED_PRINCIPAL),
        Collections.singleton(DOMAIN_URN)); // Resource type doesn't match
    assertEquals(
        _policyEngine.getGrantedPrivileges(policies, UrnUtils.getUrn(AUTHORIZED_PRINCIPAL), Optional.of(resourceSpec)),
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
    actorFilter.setUsers(new UrnArray(ImmutableList.of(Urn.createFromString("urn:li:corpuser:user1"),
        Urn.createFromString("urn:li:corpuser:user2"))));
    actorFilter.setGroups(new UrnArray(ImmutableList.of(Urn.createFromString("urn:li:corpGroup:group1"),
        Urn.createFromString("urn:li:corpGroup:group2"))));
    dataHubPolicyInfo.setActors(actorFilter);

    final DataHubResourceFilter resourceFilter = new DataHubResourceFilter();
    resourceFilter.setAllResources(false);
    resourceFilter.setType("dataset");
    StringArray resourceUrns = new StringArray();
    resourceUrns.add(RESOURCE_URN); // Filter applies to specific resource.
    resourceFilter.setResources(resourceUrns);
    dataHubPolicyInfo.setResources(resourceFilter);

    ResolvedResourceSpec resourceSpec =
        buildResourceResolvers("dataset", RESOURCE_URN, ImmutableSet.of(AUTHORIZED_PRINCIPAL, AUTHORIZED_GROUP),
            Collections.emptySet());
    PolicyEngine.PolicyActors actors = _policyEngine.getMatchingActors(dataHubPolicyInfo, Optional.of(resourceSpec));

    assertTrue(actors.allUsers());
    assertTrue(actors.allGroups());

    assertEquals(actors.getUsers(),
        ImmutableList.of(Urn.createFromString("urn:li:corpuser:user1"), Urn.createFromString("urn:li:corpuser:user2"),
            Urn.createFromString(AUTHORIZED_PRINCIPAL) // Resource Owner
        ));

    assertEquals(actors.getGroups(), ImmutableList.of(Urn.createFromString("urn:li:corpGroup:group1"),
        Urn.createFromString("urn:li:corpGroup:group2"), Urn.createFromString(AUTHORIZED_GROUP) // Resource Owner
    ));

    // Verify aspect client called, entity client not called.
    verify(_entityClient, times(0)).batchGetV2(eq(CORP_USER_ENTITY_NAME), eq(Collections.singleton(authorizedUserUrn)),
        eq(null), any());
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
    actorFilter.setUsers(new UrnArray(ImmutableList.of(Urn.createFromString("urn:li:corpuser:user1"),
        Urn.createFromString("urn:li:corpuser:user2"))));
    actorFilter.setGroups(new UrnArray(ImmutableList.of(Urn.createFromString("urn:li:corpGroup:group1"),
        Urn.createFromString("urn:li:corpGroup:group2"))));
    dataHubPolicyInfo.setActors(actorFilter);

    final DataHubResourceFilter resourceFilter = new DataHubResourceFilter();
    resourceFilter.setAllResources(false);
    resourceFilter.setType("dataset");
    StringArray resourceUrns = new StringArray();
    resourceUrns.add(RESOURCE_URN);
    resourceFilter.setResources(resourceUrns);
    dataHubPolicyInfo.setResources(resourceFilter);

    ResolvedResourceSpec resourceSpec =
        buildResourceResolvers("dataset", "urn:li:dataset:random"); // A resource not covered by the policy.
    PolicyEngine.PolicyActors actors = _policyEngine.getMatchingActors(dataHubPolicyInfo, Optional.of(resourceSpec));

    assertFalse(actors.allUsers());
    assertFalse(actors.allGroups());
    assertEquals(actors.getUsers(), Collections.emptyList());
    assertEquals(actors.getGroups(), Collections.emptyList());

    // Verify no network calls
    verify(_entityClient, times(0)).batchGetV2(any(), any(), any(), any());
  }

  private Ownership createOwnershipAspect(final Boolean addUserOwner, final Boolean addGroupOwner) throws Exception {
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
    ownershipAspect.setLastModified(new AuditStamp().setTime(0).setActor(Urn.createFromString("urn:li:corpuser:foo")));
    return ownershipAspect;
  }

  private EntityResponse createAuthorizedEntityResponse() throws URISyntaxException {
    final EntityResponse entityResponse = new EntityResponse();
    final EnvelopedAspectMap aspectMap = new EnvelopedAspectMap();

    final CorpUserInfo userInfo = new CorpUserInfo();
    userInfo.setActive(true);
    userInfo.setFullName("Data Hub");
    userInfo.setFirstName("Data");
    userInfo.setLastName("Hub");
    userInfo.setEmail("datahub@gmail.com");
    userInfo.setTitle("Admin");
    aspectMap.put(CORP_USER_INFO_ASPECT_NAME, new EnvelopedAspect().setValue(new Aspect(userInfo.data())));

    final GroupMembership groupsAspect = new GroupMembership();
    final UrnArray groups = new UrnArray();
    groups.add(Urn.createFromString("urn:li:corpGroup:authorizedGroup"));
    groupsAspect.setGroups(groups);
    aspectMap.put(GROUP_MEMBERSHIP_ASPECT_NAME, new EnvelopedAspect().setValue(new Aspect(groupsAspect.data())));

    entityResponse.setAspects(aspectMap);
    return entityResponse;
  }

  private EntityResponse createUnauthorizedEntityResponse() throws URISyntaxException {
    final EntityResponse entityResponse = new EntityResponse();
    final EnvelopedAspectMap aspectMap = new EnvelopedAspectMap();

    final CorpUserInfo userInfo = new CorpUserInfo();
    userInfo.setActive(true);
    userInfo.setFullName("Unauthorized User");
    userInfo.setFirstName("Unauthorized");
    userInfo.setLastName("User");
    userInfo.setEmail("Unauth");
    userInfo.setTitle("Engineer");
    aspectMap.put(CORP_USER_INFO_ASPECT_NAME, new EnvelopedAspect().setValue(new Aspect(userInfo.data())));

    final GroupMembership groupsAspect = new GroupMembership();
    final UrnArray groups = new UrnArray();
    groups.add(Urn.createFromString("urn:li:corpGroup:unauthorizedGroup"));
    groupsAspect.setGroups(groups);
    aspectMap.put(GROUP_MEMBERSHIP_ASPECT_NAME, new EnvelopedAspect().setValue(new Aspect(groupsAspect.data())));

    entityResponse.setAspects(aspectMap);
    return entityResponse;
  }

  public static ResolvedResourceSpec buildResourceResolvers(String entityType, String entityUrn) {
    return buildResourceResolvers(entityType, entityUrn, Collections.emptySet(), Collections.emptySet());
  }

  public static ResolvedResourceSpec buildResourceResolvers(String entityType, String entityUrn, Set<String> owners,
      Set<String> domains) {
    return new ResolvedResourceSpec(new ResourceSpec(entityType, entityUrn),
        ImmutableMap.of(ResourceFieldType.RESOURCE_TYPE, FieldResolver.getResolverFromValues(Collections.singleton(entityType)),
            ResourceFieldType.RESOURCE_URN, FieldResolver.getResolverFromValues(Collections.singleton(entityUrn)),
            ResourceFieldType.OWNER, FieldResolver.getResolverFromValues(owners), ResourceFieldType.DOMAIN,
            FieldResolver.getResolverFromValues(domains)));
  }
}
