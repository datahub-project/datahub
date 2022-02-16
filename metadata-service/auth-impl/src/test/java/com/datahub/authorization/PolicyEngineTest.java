package com.datahub.authorization;

import com.datahub.authentication.Authentication;
import com.google.common.collect.ImmutableList;
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
import com.linkedin.entity.client.OwnershipClient;
import com.linkedin.identity.CorpUserInfo;
import com.linkedin.identity.GroupMembership;
import com.linkedin.policy.DataHubActorFilter;
import com.linkedin.policy.DataHubPolicyInfo;
import com.linkedin.policy.DataHubResourceFilter;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
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

  private EntityClient _entityClient;
  private PolicyEngine _policyEngine;

  private Urn authorizedUserUrn;
  private Urn unauthorizedUserUrn;
  private Urn resourceUrn;

  @BeforeMethod
  public void setupTest() throws Exception {
    _entityClient = Mockito.mock(EntityClient.class);
    _policyEngine = new PolicyEngine(Mockito.mock(Authentication.class), _entityClient, new OwnershipClient(_entityClient));

    // Init mocks.
    EntityResponse authorizedEntityResponse = createAuthorizedEntityResponse();
    authorizedUserUrn = Urn.createFromString(AUTHORIZED_PRINCIPAL);
    authorizedEntityResponse.setUrn(authorizedUserUrn);
    Map<Urn, EntityResponse> authorizedEntityResponseMap = Collections.singletonMap(authorizedUserUrn, authorizedEntityResponse);
    when(_entityClient.batchGetV2(eq(CORP_USER_ENTITY_NAME), eq(Collections.singleton(authorizedUserUrn)),
        eq(null), any())).thenReturn(authorizedEntityResponseMap);

    EntityResponse unauthorizedEntityResponse = createUnauthorizedEntityResponse();
    unauthorizedUserUrn = Urn.createFromString(UNAUTHORIZED_PRINCIPAL);
    unauthorizedEntityResponse.setUrn(unauthorizedUserUrn);
    Map<Urn, EntityResponse> unauthorizedEntityResponseMap = Collections.singletonMap(unauthorizedUserUrn, unauthorizedEntityResponse);
    when(_entityClient.batchGetV2(eq(CORP_USER_ENTITY_NAME), eq(Collections.singleton(unauthorizedUserUrn)),
        eq(null), any())).thenReturn(unauthorizedEntityResponseMap);

    EntityResponse entityResponse = new EntityResponse();
    EnvelopedAspectMap envelopedAspectMap = new EnvelopedAspectMap();
    envelopedAspectMap.put(OWNERSHIP_ASPECT_NAME, new EnvelopedAspect()
        .setValue(new com.linkedin.entity.Aspect(createOwnershipAspect(true, true).data())));
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

    PolicyEngine.PolicyEvaluationResult result = _policyEngine.evaluatePolicy(
        dataHubPolicyInfo, AUTHORIZED_PRINCIPAL,
        "EDIT_ENTITY_TAGS",
        Optional.of(new ResourceSpec(
            "dataset",
            RESOURCE_URN
        ))
    );

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

    PolicyEngine.PolicyEvaluationResult result = _policyEngine.evaluatePolicy(
        dataHubPolicyInfo,
        AUTHORIZED_PRINCIPAL,
        "EDIT_ENTITY_OWNERS",
        Optional.of(new ResourceSpec(
            "dataset",
            RESOURCE_URN
        ))
    );
    assertFalse(result.isGranted());

    // Verify no network calls
    verify(_entityClient, times(0)).batchGetV2(
        any(), any(), any(), any());
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

    PolicyEngine.PolicyEvaluationResult result = _policyEngine.evaluatePolicy(
        dataHubPolicyInfo,
        AUTHORIZED_PRINCIPAL,
        "MANAGE_POLICIES",
        Optional.empty()
    );
    assertTrue(result.isGranted());

    // Verify no network calls
    verify(_entityClient, times(0)).batchGetV2(
        any(), any(), any(), any());
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

    // Assert Authorized user can edit entity tags.
    PolicyEngine.PolicyEvaluationResult result1 = _policyEngine.evaluatePolicy(
        dataHubPolicyInfo, AUTHORIZED_PRINCIPAL,
        "EDIT_ENTITY_TAGS",
        Optional.of(new ResourceSpec(
            "dataset",
            RESOURCE_URN
        ))
    );

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

    // Assert unauthorized user cannot edit entity tags.
    PolicyEngine.PolicyEvaluationResult result2 = _policyEngine.evaluatePolicy(
        dataHubPolicyInfo,
        "urn:li:corpuser:test",
        "EDIT_ENTITY_TAGS",
        Optional.of(new ResourceSpec(
            "dataset",
            RESOURCE_URN
        ))
    );

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

    // Assert authorized user can edit entity tags, because of group membership.
    PolicyEngine.PolicyEvaluationResult result1 = _policyEngine.evaluatePolicy(
        dataHubPolicyInfo, AUTHORIZED_PRINCIPAL,
        "EDIT_ENTITY_TAGS",
        Optional.of(new ResourceSpec(
            "dataset",
            RESOURCE_URN
        ))
    );
    assertTrue(result1.isGranted());

    // Verify we are only calling for group during these requests.
    verify(_entityClient, times(1)).batchGetV2(eq(CORP_USER_ENTITY_NAME),
        eq(Collections.singleton(authorizedUserUrn)), eq(null), any());
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

    // Assert unauthorized user cannot edit entity tags.
    PolicyEngine.PolicyEvaluationResult result2 = _policyEngine.evaluatePolicy(
        dataHubPolicyInfo,
        UNAUTHORIZED_PRINCIPAL,
        "EDIT_ENTITY_TAGS",
        Optional.of(new ResourceSpec(
            "dataset",
            RESOURCE_URN
        ))
    );
    assertFalse(result2.isGranted());

    // Verify we are only calling for group during these requests.
    verify(_entityClient, times(1)).batchGetV2(eq(CORP_USER_ENTITY_NAME),
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

    // Assert authorized user can edit entity tags, because of group membership.
    PolicyEngine.PolicyEvaluationResult result1 = _policyEngine.evaluatePolicy(
        dataHubPolicyInfo, AUTHORIZED_PRINCIPAL,
        "EDIT_ENTITY_TAGS",
        Optional.of(new ResourceSpec(
            "dataset",
            RESOURCE_URN
        ))
    );
    assertTrue(result1.isGranted());

    // Assert unauthorized user cannot edit entity tags.
    PolicyEngine.PolicyEvaluationResult result2 = _policyEngine.evaluatePolicy(
        dataHubPolicyInfo,
        UNAUTHORIZED_PRINCIPAL,
        "EDIT_ENTITY_TAGS",
        Optional.of(new ResourceSpec(
            "dataset",
            RESOURCE_URN
        ))
    );
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

    // Assert authorized user can edit entity tags, because of group membership.
    PolicyEngine.PolicyEvaluationResult result1 = _policyEngine.evaluatePolicy(
        dataHubPolicyInfo, AUTHORIZED_PRINCIPAL,
        "EDIT_ENTITY_TAGS",
        Optional.of(new ResourceSpec(
            "dataset",
            RESOURCE_URN
        ))
    );
    assertTrue(result1.isGranted());

    // Assert unauthorized user cannot edit entity tags.
    PolicyEngine.PolicyEvaluationResult result2 = _policyEngine.evaluatePolicy(
        dataHubPolicyInfo,
        UNAUTHORIZED_PRINCIPAL,
        "EDIT_ENTITY_TAGS",
        Optional.of(new ResourceSpec(
            "dataset",
            RESOURCE_URN
        ))
    );
    assertTrue(result2.isGranted());

    // Verify we are only calling for group during these requests.
    verify(_entityClient, times(1)).batchGetV2(eq(CORP_USER_ENTITY_NAME),
        eq(Collections.singleton(authorizedUserUrn)), eq(null), any());
    verify(_entityClient, times(1)).batchGetV2(eq(CORP_USER_ENTITY_NAME),
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

    // Assert authorized user can edit entity tags, because he is a user owner.
    PolicyEngine.PolicyEvaluationResult result1 = _policyEngine.evaluatePolicy(
        dataHubPolicyInfo, AUTHORIZED_PRINCIPAL,
        "EDIT_ENTITY_TAGS",
        Optional.of(new ResourceSpec(
            "dataset",
            RESOURCE_URN
        ))
    );
    assertTrue(result1.isGranted());

    // Verify we are calling for the resource ownership aspect
    verify(_entityClient, times(1)).batchGetV2(
        any(),
        eq(Collections.singleton(resourceUrn)),
        eq(Collections.singleton(OWNERSHIP_ASPECT_NAME)),
        any());
    // Ensure no calls for group membership.
    verify(_entityClient, times(0)).batchGetV2(
        eq(CORP_USER_ENTITY_NAME),
        eq(Collections.singleton(authorizedUserUrn)),
        eq(null),
        any());
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

    // Overwrite the Ownership of the Resource to only include a single group.
    EntityResponse entityResponse = new EntityResponse();
    EnvelopedAspectMap envelopedAspectMap = new EnvelopedAspectMap();
    envelopedAspectMap.put(OWNERSHIP_ASPECT_NAME, new EnvelopedAspect()
        .setValue(new com.linkedin.entity.Aspect(createOwnershipAspect(false, true).data())));
    entityResponse.setAspects(envelopedAspectMap);
    Map<Urn, EntityResponse> mockMap = mock(Map.class);
    when(_entityClient.batchGetV2(any(), eq(Collections.singleton(resourceUrn)),
        eq(Collections.singleton(OWNERSHIP_ASPECT_NAME)), any())).thenReturn(mockMap);
    when(mockMap.get(any(Urn.class))).thenReturn(entityResponse);

    // Assert authorized user can edit entity tags, because he is a user owner.
    PolicyEngine.PolicyEvaluationResult result1 = _policyEngine.evaluatePolicy(
        dataHubPolicyInfo, AUTHORIZED_PRINCIPAL,
        "EDIT_ENTITY_TAGS",
        Optional.of(new ResourceSpec(
            "dataset",
            RESOURCE_URN
        ))
    );
    assertTrue(result1.isGranted());

    // Verify we are calling for the resource ownership aspect
    verify(_entityClient, times(1)).batchGetV2(
        any(),
        eq(Collections.singleton(resourceUrn)),
        eq(Collections.singleton(OWNERSHIP_ASPECT_NAME)),
        any());
    // Ensure that caching of groups is working with 1 call to entity client for each principal.
    verify(_entityClient, times(1)).batchGetV2(eq(CORP_USER_ENTITY_NAME),
        eq(Collections.singleton(authorizedUserUrn)), eq(null), any());
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

    // Assert unauthorized user cannot edit entity tags.
    PolicyEngine.PolicyEvaluationResult result2 = _policyEngine.evaluatePolicy(
        dataHubPolicyInfo,
        UNAUTHORIZED_PRINCIPAL,
        "EDIT_ENTITY_TAGS",
        Optional.of(new ResourceSpec(
            "dataset",
            RESOURCE_URN
        ))
    );
    assertFalse(result2.isGranted());

    // Verify we are calling for the resource ownership aspect
    verify(_entityClient, times(1)).batchGetV2(
        any(),
        eq(Collections.singleton(resourceUrn)),
        eq(Collections.singleton(OWNERSHIP_ASPECT_NAME)),
        any());
    // Ensure that caching of groups is working with 1 call to entity client for each principal.
    verify(_entityClient, times(1)).batchGetV2(eq(CORP_USER_ENTITY_NAME),
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

    PolicyEngine.PolicyEvaluationResult result = _policyEngine.evaluatePolicy(
        dataHubPolicyInfo,
        AUTHORIZED_PRINCIPAL,
        "EDIT_ENTITY_TAGS",
        Optional.of(new ResourceSpec(
            "dataset",
            "urn:li:dataset:random" // A dataset Authorized principal _does not own_.
        ))
    );
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

    PolicyEngine.PolicyEvaluationResult result = _policyEngine.evaluatePolicy(
        dataHubPolicyInfo,
        AUTHORIZED_PRINCIPAL,
        "EDIT_ENTITY_TAGS",
        Optional.of(new ResourceSpec(
            "chart", // Notice: Not a dataset.
            RESOURCE_URN
        ))
    );
    assertFalse(result.isGranted());

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
    resourceFilter.setAllResources(false);
    resourceFilter.setType("dataset");

    StringArray resourceUrns = new StringArray();
    resourceUrns.add(RESOURCE_URN); // Filter applies to specific resource.
    resourceFilter.setResources(resourceUrns);
    dataHubPolicyInfo.setResources(resourceFilter);

    PolicyEngine.PolicyEvaluationResult result = _policyEngine.evaluatePolicy(
        dataHubPolicyInfo,
        AUTHORIZED_PRINCIPAL,
        "EDIT_ENTITY_TAGS",
        Optional.of(new ResourceSpec(
            "dataset",
            RESOURCE_URN
        ))
    );
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
    resourceFilter.setAllResources(false);
    resourceFilter.setType("dataset");

    StringArray resourceUrns = new StringArray();
    resourceUrns.add(RESOURCE_URN); // Filter applies to specific resource.
    resourceFilter.setResources(resourceUrns);
    dataHubPolicyInfo.setResources(resourceFilter);

    PolicyEngine.PolicyEvaluationResult result = _policyEngine.evaluatePolicy(
        dataHubPolicyInfo,
        AUTHORIZED_PRINCIPAL,
        "EDIT_ENTITY_TAGS",
        Optional.of(new ResourceSpec(
            "dataset",
            "urn:li:dataset:random" // A resource not covered by the policy.
        ))
    );
    assertFalse(result.isGranted());

    // Verify no network calls
    verify(_entityClient, times(0)).batchGetV2(any(), any(), any(), any());
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
    actorFilter.setUsers(new UrnArray(
        ImmutableList.of(
            Urn.createFromString("urn:li:corpuser:user1"),
            Urn.createFromString("urn:li:corpuser:user2")
        )
    ));
    actorFilter.setGroups(new UrnArray(
        ImmutableList.of(
            Urn.createFromString("urn:li:corpGroup:group1"),
            Urn.createFromString("urn:li:corpGroup:group2")
        )
    ));
    dataHubPolicyInfo.setActors(actorFilter);

    final DataHubResourceFilter resourceFilter = new DataHubResourceFilter();
    resourceFilter.setAllResources(false);
    resourceFilter.setType("dataset");
    StringArray resourceUrns = new StringArray();
    resourceUrns.add(RESOURCE_URN); // Filter applies to specific resource.
    resourceFilter.setResources(resourceUrns);
    dataHubPolicyInfo.setResources(resourceFilter);

    PolicyEngine.PolicyActors actors = _policyEngine.getMatchingActors(
        dataHubPolicyInfo,
        Optional.of(new ResourceSpec(
            "dataset",
            RESOURCE_URN // A resource covered by the policy.
        ))
    );

    assertTrue(actors.allUsers());
    assertTrue(actors.allGroups());

    assertEquals(actors.getUsers(), ImmutableList.of(
        Urn.createFromString("urn:li:corpuser:user1"),
        Urn.createFromString("urn:li:corpuser:user2"),
        Urn.createFromString(AUTHORIZED_PRINCIPAL) // Resource Owner
    ));

    assertEquals(actors.getGroups(), ImmutableList.of(
        Urn.createFromString("urn:li:corpGroup:group1"),
        Urn.createFromString("urn:li:corpGroup:group2"),
        Urn.createFromString(AUTHORIZED_GROUP) // Resource Owner
    ));

    // Verify aspect client called, entity client not called.
    verify(_entityClient, times(1)).batchGetV2(any(), any(), any(), any());
    verify(_entityClient, times(0)).batchGetV2(eq(CORP_USER_ENTITY_NAME),
        eq(Collections.singleton(authorizedUserUrn)), eq(null), any());
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
    actorFilter.setUsers(new UrnArray(
        ImmutableList.of(
            Urn.createFromString("urn:li:corpuser:user1"),
            Urn.createFromString("urn:li:corpuser:user2")
        )
    ));
    actorFilter.setGroups(new UrnArray(
        ImmutableList.of(
            Urn.createFromString("urn:li:corpGroup:group1"),
            Urn.createFromString("urn:li:corpGroup:group2")
        )
    ));
    dataHubPolicyInfo.setActors(actorFilter);

    final DataHubResourceFilter resourceFilter = new DataHubResourceFilter();
    resourceFilter.setAllResources(false);
    resourceFilter.setType("dataset");
    StringArray resourceUrns = new StringArray();
    resourceUrns.add(RESOURCE_URN);
    resourceFilter.setResources(resourceUrns);
    dataHubPolicyInfo.setResources(resourceFilter);

    PolicyEngine.PolicyActors actors = _policyEngine.getMatchingActors(
        dataHubPolicyInfo,
        Optional.of(new ResourceSpec(
            "dataset",
            "urn:li:dataset:random" // A resource not covered by the policy.
        ))
    );

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
}
