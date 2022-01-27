package com.datahub.authorization;

import com.datahub.authentication.Authentication;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
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
import com.linkedin.metadata.query.ListUrnsResult;
import com.linkedin.policy.DataHubActorFilter;
import com.linkedin.policy.DataHubPolicyInfo;
import com.linkedin.policy.DataHubResourceFilter;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.linkedin.metadata.Constants.*;
import static com.linkedin.metadata.authorization.PoliciesConfig.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;


public class AuthorizationManagerTest {

  private EntityClient _entityClient;
  private AuthorizationManager _authorizationManager;

  @BeforeMethod
  public void setupTest() throws Exception {
    _entityClient = Mockito.mock(EntityClient.class);

    // Init mocks.
    final Urn activePolicyUrn = Urn.createFromString("urn:li:dataHubPolicy:0");
    final DataHubPolicyInfo activePolicy = createDataHubPolicyInfo(true, ImmutableList.of("EDIT_ENTITY_TAGS"));
    final EnvelopedAspectMap activeAspectMap = new EnvelopedAspectMap();
    activeAspectMap.put(DATAHUB_POLICY_INFO_ASPECT_NAME, new EnvelopedAspect().setValue(new Aspect(activePolicy.data())));

    final Urn inactivePolicyUrn = Urn.createFromString("urn:li:dataHubPolicy:1");
    final DataHubPolicyInfo inactivePolicy = createDataHubPolicyInfo(false, ImmutableList.of("EDIT_ENTITY_OWNERS"));
    final EnvelopedAspectMap inactiveAspectMap = new EnvelopedAspectMap();
    inactiveAspectMap.put(DATAHUB_POLICY_INFO_ASPECT_NAME, new EnvelopedAspect().setValue(new Aspect(inactivePolicy.data())));

    final ListUrnsResult listUrnsResult = new ListUrnsResult();
    listUrnsResult.setStart(0);
    listUrnsResult.setTotal(2);
    listUrnsResult.setCount(2);
    UrnArray policyUrns = new UrnArray(ImmutableList.of(
        activePolicyUrn,
        inactivePolicyUrn));
    listUrnsResult.setEntities(policyUrns);

    when(_entityClient.listUrns(eq("dataHubPolicy"), eq(0), anyInt(), any())).thenReturn(listUrnsResult);
    when(_entityClient.batchGetV2(eq(POLICY_ENTITY_NAME),
        eq(new HashSet<>(listUrnsResult.getEntities())), eq(null), any())).thenReturn(
        ImmutableMap.of(
            activePolicyUrn, new EntityResponse().setUrn(activePolicyUrn).setAspects(activeAspectMap),
            inactivePolicyUrn, new EntityResponse().setUrn(inactivePolicyUrn).setAspects(inactiveAspectMap)
        )
    );

    final List<Urn> userUrns = ImmutableList.of(Urn.createFromString("urn:li:corpuser:user3"), Urn.createFromString("urn:li:corpuser:user4"));
    final List<Urn> groupUrns = ImmutableList.of(Urn.createFromString("urn:li:corpGroup:group3"), Urn.createFromString("urn:li:corpGroup:group4"));
    EntityResponse entityResponse = new EntityResponse();
    EnvelopedAspectMap envelopedAspectMap = new EnvelopedAspectMap();
    envelopedAspectMap.put(OWNERSHIP_ASPECT_NAME, new EnvelopedAspect()
        .setValue(new com.linkedin.entity.Aspect(createOwnershipAspect(userUrns, groupUrns).data())));
    entityResponse.setAspects(envelopedAspectMap);
    Map<Urn, EntityResponse> mockMap = mock(Map.class);
    when(_entityClient.batchGetV2(any(), any(), eq(Collections.singleton(OWNERSHIP_ASPECT_NAME)), any()))
        .thenReturn(mockMap);
    when(mockMap.get(any(Urn.class))).thenReturn(entityResponse);

    _authorizationManager = new AuthorizationManager(
        Mockito.mock(Authentication.class),
        _entityClient,
        new OwnershipClient(_entityClient),
        10,
        10,
        Authorizer.AuthorizationMode.DEFAULT
    );
    _authorizationManager.invalidateCache();
    Thread.sleep(500); // Sleep so the runnable can execute. (not ideal)
  }

  @Test
  public void testAuthorizeGranted() throws Exception {

    ResourceSpec resourceSpec = new ResourceSpec("dataset", "urn:li:dataset:test");

    AuthorizationRequest request = new AuthorizationRequest(
        "urn:li:corpuser:test",
        "EDIT_ENTITY_TAGS",
        Optional.of(resourceSpec)
      );

    assertEquals(_authorizationManager.authorize(request).getType(), AuthorizationResult.Type.ALLOW);
  }

  @Test
  public void testAuthorizeNotGranted() throws Exception {

    ResourceSpec resourceSpec = new ResourceSpec("dataset", "urn:li:dataset:test");

    // Policy for this privilege is inactive.
    AuthorizationRequest request = new AuthorizationRequest(
        "urn:li:corpuser:test",
        "EDIT_ENTITY_OWNERS",
        Optional.of(resourceSpec)
    );

    assertEquals(_authorizationManager.authorize(request).getType(), AuthorizationResult.Type.DENY);
  }

  @Test
  public void testAllowAllMode() throws Exception {

    _authorizationManager.setMode(Authorizer.AuthorizationMode.ALLOW_ALL);

    ResourceSpec resourceSpec = new ResourceSpec("dataset", "urn:li:dataset:test");

    // Policy for this privilege is inactive.
    AuthorizationRequest request = new AuthorizationRequest(
        "urn:li:corpuser:test",
        "EDIT_ENTITY_OWNERS",
        Optional.of(resourceSpec)
    );

    assertEquals(_authorizationManager.authorize(request).getType(), AuthorizationResult.Type.ALLOW);
  }

  @Test
  public void testInvalidateCache() throws Exception {

    // First make sure that the default policies are as expected.
    ResourceSpec resourceSpec = new ResourceSpec("dataset", "urn:li:dataset:test");

    AuthorizationRequest request = new AuthorizationRequest(
        "urn:li:corpuser:test",
        "EDIT_ENTITY_TAGS",
        Optional.of(resourceSpec)
    );

    assertEquals(_authorizationManager.authorize(request).getType(), AuthorizationResult.Type.ALLOW);

    // Now init the mocks to return 0 policies.
    final ListUrnsResult emptyUrnsResult = new ListUrnsResult();
    emptyUrnsResult.setStart(0);
    emptyUrnsResult.setTotal(0);
    emptyUrnsResult.setCount(0);
    emptyUrnsResult.setEntities(new UrnArray(Collections.emptyList()));

    when(_entityClient.listUrns(eq("dataHubPolicy"), eq(0), anyInt(), any())).thenReturn(emptyUrnsResult);
    when(_entityClient.batchGetV2(eq(POLICY_ENTITY_NAME), eq(new HashSet<>(emptyUrnsResult.getEntities())),
        eq(null), any())).thenReturn(
        Collections.emptyMap()
    );

    // Invalidate Cache.
    _authorizationManager.invalidateCache();
    Thread.sleep(500); // Sleep so the runnable can execute. (not ideal)
    // Now verify that invalidating the cache updates the policies by running the same authorization request.
    assertEquals(_authorizationManager.authorize(request).getType(), AuthorizationResult.Type.DENY);
  }

  @Test
  public void testAuthorizedActorsActivePolicy() throws Exception {

    final AuthorizationManager.AuthorizedActors actors = _authorizationManager.authorizedActors(
        "EDIT_ENTITY_TAGS", // Should be inside the active policy.
        Optional.of(new ResourceSpec("dataset", "urn:li:dataset:1"))
    );

    assertTrue(actors.allUsers());
    assertTrue(actors.allGroups());

    assertEquals(actors.getUsers(), ImmutableList.of(
        Urn.createFromString("urn:li:corpuser:user1"),
        Urn.createFromString("urn:li:corpuser:user2"),
        Urn.createFromString("urn:li:corpuser:user3"),
        Urn.createFromString("urn:li:corpuser:user4")
    ));

    assertEquals(actors.getGroups(), ImmutableList.of(
        Urn.createFromString("urn:li:corpGroup:group1"),
        Urn.createFromString("urn:li:corpGroup:group2"),
        Urn.createFromString("urn:li:corpGroup:group3"),
        Urn.createFromString("urn:li:corpGroup:group4")
    ));
  }

  private DataHubPolicyInfo createDataHubPolicyInfo(boolean active, List<String> privileges) throws Exception {
    final DataHubPolicyInfo dataHubPolicyInfo = new DataHubPolicyInfo();
    dataHubPolicyInfo.setType(METADATA_POLICY_TYPE);
    dataHubPolicyInfo.setState(active ? ACTIVE_POLICY_STATE : INACTIVE_POLICY_STATE);
    dataHubPolicyInfo.setPrivileges(new StringArray(privileges));
    dataHubPolicyInfo.setDisplayName("My Test Display");
    dataHubPolicyInfo.setDescription("My test display!");
    dataHubPolicyInfo.setEditable(true);

    List<Urn> users = ImmutableList.of(Urn.createFromString("urn:li:corpuser:user1"), Urn.createFromString("urn:li:corpuser:user2"));
    List<Urn> groups = ImmutableList.of(Urn.createFromString("urn:li:corpGroup:group1"), Urn.createFromString("urn:li:corpGroup:group2"));

    final DataHubActorFilter actorFilter = new DataHubActorFilter();
    actorFilter.setResourceOwners(true);
    actorFilter.setAllUsers(true);
    actorFilter.setAllGroups(true);
    actorFilter.setUsers(new UrnArray(users));
    actorFilter.setGroups(new UrnArray(groups));
    dataHubPolicyInfo.setActors(actorFilter);

    final DataHubResourceFilter resourceFilter = new DataHubResourceFilter();
    resourceFilter.setAllResources(true);
    resourceFilter.setType("dataset");
    dataHubPolicyInfo.setResources(resourceFilter);
    return dataHubPolicyInfo;
  }

  private Ownership createOwnershipAspect(final List<Urn> userOwners, final List<Urn> groupOwners) throws Exception {
    final Ownership ownershipAspect = new Ownership();
    final OwnerArray owners = new OwnerArray();

    if (userOwners != null) {
      userOwners.forEach(userUrn -> {
          final Owner userOwner = new Owner();
          userOwner.setOwner(userUrn);
          userOwner.setType(OwnershipType.DATAOWNER);
          owners.add(userOwner);
        }
      );
    }

    if (groupOwners != null) {
      groupOwners.forEach(groupUrn -> {
        final Owner groupOwner = new Owner();
        groupOwner.setOwner(groupUrn);
        groupOwner.setType(OwnershipType.DATAOWNER);
        owners.add(groupOwner);
      });
    }

    ownershipAspect.setOwners(owners);
    ownershipAspect.setLastModified(new AuditStamp().setTime(0).setActor(Urn.createFromString("urn:li:corpuser:foo")));
    return ownershipAspect;
  }
}
