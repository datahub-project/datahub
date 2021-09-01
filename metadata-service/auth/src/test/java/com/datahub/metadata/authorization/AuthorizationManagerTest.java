package com.datahub.metadata.authorization;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.StringArray;
import com.linkedin.entity.Entity;
import com.linkedin.entity.client.AspectClient;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.aspect.DataHubPolicyAspect;
import com.linkedin.metadata.aspect.DataHubPolicyAspectArray;
import com.linkedin.metadata.query.ListUrnsResult;
import com.linkedin.metadata.snapshot.DataHubPolicySnapshot;
import com.linkedin.metadata.snapshot.Snapshot;
import com.linkedin.policy.DataHubActorFilter;
import com.linkedin.policy.DataHubPolicyInfo;
import com.linkedin.policy.DataHubResourceFilter;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;
import static com.linkedin.metadata.Constants.*;
import static com.linkedin.metadata.authorization.PoliciesConfig.*;


public class AuthorizationManagerTest {

  private EntityClient _entityClient;
  private AspectClient _aspectClient;
  private AuthorizationManager _authorizationManager;

  @BeforeMethod
  public void setupTest() throws Exception {
    _entityClient = Mockito.mock(EntityClient.class);
    _aspectClient = Mockito.mock(AspectClient.class);

    // Init mocks.
    final Urn activePolicyUrn = Urn.createFromString("urn:li:dataHubPolicy:0");
    final DataHubPolicyInfo activePolicy = createDataHubPolicyInfo(true, ImmutableList.of("EDIT_ENTITY_TAGS"));
    final DataHubPolicySnapshot activePolicySnapshot = new DataHubPolicySnapshot();
    activePolicySnapshot.setUrn(activePolicyUrn);
    activePolicySnapshot.setAspects(new DataHubPolicyAspectArray(ImmutableList.of(
        DataHubPolicyAspect.create(activePolicy)
    )));

    final Urn inactivePolicyUrn = Urn.createFromString("urn:li:dataHubPolicy:1");
    final DataHubPolicyInfo inactivePolicy = createDataHubPolicyInfo(false, ImmutableList.of("EDIT_ENTITY_OWNERS"));
    final DataHubPolicySnapshot inactivePolicySnapshot = new DataHubPolicySnapshot();
    inactivePolicySnapshot.setUrn(inactivePolicyUrn);
    inactivePolicySnapshot.setAspects(new DataHubPolicyAspectArray(ImmutableList.of(
        DataHubPolicyAspect.create(inactivePolicy)
    )));

    final ListUrnsResult listUrnsResult = new ListUrnsResult();
    listUrnsResult.setStart(0);
    listUrnsResult.setTotal(2);
    listUrnsResult.setCount(2);
    UrnArray policyUrns = new UrnArray(ImmutableList.of(
        activePolicyUrn,
        inactivePolicyUrn));
    listUrnsResult.setEntities(policyUrns);

    when(_entityClient.listUrns(eq("dataHubPolicy"), eq(0), anyInt(), any())).thenReturn(listUrnsResult);
    when(_entityClient.batchGet(eq(new HashSet<>(listUrnsResult.getEntities())), any())).thenReturn(
        ImmutableMap.of(
            activePolicyUrn, new Entity().setValue(Snapshot.create(activePolicySnapshot)),
            inactivePolicyUrn, new Entity().setValue(Snapshot.create(inactivePolicySnapshot))
        )
    );

    _authorizationManager = new AuthorizationManager(
        _entityClient,
        _aspectClient,
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

    assertEquals(_authorizationManager.authorize(request).getType(), AuthorizationResult.Type.DENY);
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
    when(_entityClient.batchGet(eq(new HashSet<>(emptyUrnsResult.getEntities())), any())).thenReturn(
        Collections.emptyMap()
    );

    // Invalidate Cache.
    _authorizationManager.invalidateCache();
    Thread.sleep(500); // Sleep so the runnable can execute. (not ideal)
    // Now verify that invalidating the cache updates the policies by running the same authorization request.
    assertEquals(_authorizationManager.authorize(request).getType(), AuthorizationResult.Type.DENY);
  }

  private DataHubPolicyInfo createDataHubPolicyInfo(boolean active, List<String> privileges) {
    final DataHubPolicyInfo dataHubPolicyInfo = new DataHubPolicyInfo();
    dataHubPolicyInfo.setType(METADATA_POLICY_TYPE);
    dataHubPolicyInfo.setState(active ? ACTIVE_POLICY_STATE : INACTIVE_POLICY_STATE);
    dataHubPolicyInfo.setPrivileges(new StringArray(privileges));
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
    return dataHubPolicyInfo;
  }
}
