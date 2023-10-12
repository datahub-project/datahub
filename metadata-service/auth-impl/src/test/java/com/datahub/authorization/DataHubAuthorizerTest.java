package com.datahub.authorization;
import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
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
import com.linkedin.domain.DomainProperties;
import com.linkedin.domain.Domains;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.query.SearchFlags;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.policy.DataHubActorFilter;
import com.linkedin.policy.DataHubPolicyInfo;
import com.linkedin.policy.DataHubResourceFilter;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.annotation.Nullable;

import static com.linkedin.metadata.Constants.*;
import static com.linkedin.metadata.authorization.PoliciesConfig.ACTIVE_POLICY_STATE;
import static com.linkedin.metadata.authorization.PoliciesConfig.INACTIVE_POLICY_STATE;
import static com.linkedin.metadata.authorization.PoliciesConfig.METADATA_POLICY_TYPE;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


public class DataHubAuthorizerTest {

  public static final String DATAHUB_SYSTEM_CLIENT_ID = "__datahub_system";

  private static final Urn PARENT_DOMAIN_URN = UrnUtils.getUrn("urn:li:domain:parent");
  private static final Urn CHILD_DOMAIN_URN = UrnUtils.getUrn("urn:li:domain:child");

  private EntityClient _entityClient;
  private DataHubAuthorizer _dataHubAuthorizer;

  @BeforeMethod
  public void setupTest() throws Exception {
    _entityClient = mock(EntityClient.class);

    // Init mocks.
    final Urn activePolicyUrn = Urn.createFromString("urn:li:dataHubPolicy:0");
    final DataHubPolicyInfo activePolicy = createDataHubPolicyInfo(true, ImmutableList.of("EDIT_ENTITY_TAGS"), null);
    final EnvelopedAspectMap activeAspectMap = new EnvelopedAspectMap();
    activeAspectMap.put(DATAHUB_POLICY_INFO_ASPECT_NAME, new EnvelopedAspect().setValue(new Aspect(activePolicy.data())));

    final Urn inactivePolicyUrn = Urn.createFromString("urn:li:dataHubPolicy:1");
    final DataHubPolicyInfo inactivePolicy = createDataHubPolicyInfo(false, ImmutableList.of("EDIT_ENTITY_OWNERS"), null);
    final EnvelopedAspectMap inactiveAspectMap = new EnvelopedAspectMap();
    inactiveAspectMap.put(DATAHUB_POLICY_INFO_ASPECT_NAME, new EnvelopedAspect().setValue(new Aspect(inactivePolicy.data())));

    final Urn parentDomainPolicyUrn = Urn.createFromString("urn:li:dataHubPolicy:2");
    final DataHubPolicyInfo parentDomainPolicy = createDataHubPolicyInfo(true, ImmutableList.of("EDIT_ENTITY_DOCS"), PARENT_DOMAIN_URN);
    final EnvelopedAspectMap parentDomainPolicyAspectMap = new EnvelopedAspectMap();
    parentDomainPolicyAspectMap.put(DATAHUB_POLICY_INFO_ASPECT_NAME, new EnvelopedAspect().setValue(new Aspect(parentDomainPolicy.data())));

    final Urn childDomainPolicyUrn = Urn.createFromString("urn:li:dataHubPolicy:3");
    final DataHubPolicyInfo childDomainPolicy = createDataHubPolicyInfo(true, ImmutableList.of("EDIT_ENTITY_STATUS"), CHILD_DOMAIN_URN);
    final EnvelopedAspectMap childDomainPolicyAspectMap = new EnvelopedAspectMap();
    childDomainPolicyAspectMap.put(DATAHUB_POLICY_INFO_ASPECT_NAME, new EnvelopedAspect().setValue(new Aspect(childDomainPolicy.data())));

    final SearchResult policySearchResult = new SearchResult();
    policySearchResult.setNumEntities(3);
    policySearchResult.setEntities(
        new SearchEntityArray(
            ImmutableList.of(
                new SearchEntity().setEntity(activePolicyUrn),
                new SearchEntity().setEntity(inactivePolicyUrn),
                new SearchEntity().setEntity(parentDomainPolicyUrn),
                new SearchEntity().setEntity(childDomainPolicyUrn)
            )
        )
    );

    when(_entityClient.search(eq("dataHubPolicy"), eq(""), isNull(), any(), anyInt(), anyInt(), any(),
        eq(new SearchFlags().setFulltext(true)))).thenReturn(policySearchResult);
    when(_entityClient.batchGetV2(eq(POLICY_ENTITY_NAME),
        eq(ImmutableSet.of(activePolicyUrn, inactivePolicyUrn, parentDomainPolicyUrn, childDomainPolicyUrn)), eq(null), any())).thenReturn(
        ImmutableMap.of(
            activePolicyUrn, new EntityResponse().setUrn(activePolicyUrn).setAspects(activeAspectMap),
            inactivePolicyUrn, new EntityResponse().setUrn(inactivePolicyUrn).setAspects(inactiveAspectMap),
            parentDomainPolicyUrn, new EntityResponse().setUrn(parentDomainPolicyUrn).setAspects(parentDomainPolicyAspectMap),
            childDomainPolicyUrn, new EntityResponse().setUrn(childDomainPolicyUrn).setAspects(childDomainPolicyAspectMap)
        )
    );

    final List<Urn> userUrns = ImmutableList.of(Urn.createFromString("urn:li:corpuser:user3"), Urn.createFromString("urn:li:corpuser:user4"));
    final List<Urn> groupUrns = ImmutableList.of(Urn.createFromString("urn:li:corpGroup:group3"), Urn.createFromString("urn:li:corpGroup:group4"));
    EntityResponse ownershipResponse = new EntityResponse();
    EnvelopedAspectMap ownershipAspectMap = new EnvelopedAspectMap();
    ownershipAspectMap.put(OWNERSHIP_ASPECT_NAME, new EnvelopedAspect()
        .setValue(new com.linkedin.entity.Aspect(createOwnershipAspect(userUrns, groupUrns).data())));
    ownershipResponse.setAspects(ownershipAspectMap);
    when(_entityClient.getV2(any(), any(), eq(Collections.singleton(OWNERSHIP_ASPECT_NAME)), any()))
        .thenReturn(ownershipResponse);

    // Mocks to get domains on a resource
    when(_entityClient.getV2(any(), any(), eq(Collections.singleton(DOMAINS_ASPECT_NAME)), any()))
        .thenReturn(createDomainsResponse(CHILD_DOMAIN_URN));

    // Mocks to get parent domains on a domain
    when(_entityClient.batchGetV2(any(), eq(Collections.singleton(CHILD_DOMAIN_URN)), eq(Collections.singleton(DOMAIN_PROPERTIES_ASPECT_NAME)), any()))
        .thenReturn(createDomainPropertiesBatchResponse(PARENT_DOMAIN_URN));

    // Mocks to reach the stopping point on domain parents
    when(_entityClient.batchGetV2(any(), eq(Collections.singleton(PARENT_DOMAIN_URN)), eq(Collections.singleton(DOMAIN_PROPERTIES_ASPECT_NAME)), any()))
        .thenReturn(createDomainPropertiesBatchResponse(null));

    final Authentication systemAuthentication = new Authentication(
        new Actor(ActorType.USER, DATAHUB_SYSTEM_CLIENT_ID),
        ""
    );

    _dataHubAuthorizer = new DataHubAuthorizer(
        systemAuthentication,
        _entityClient,
        10,
        10,
        DataHubAuthorizer.AuthorizationMode.DEFAULT
    );
    _dataHubAuthorizer.init(Collections.emptyMap(), createAuthorizerContext(systemAuthentication, _entityClient));
    _dataHubAuthorizer.invalidateCache();
    Thread.sleep(500); // Sleep so the runnable can execute. (not ideal)
  }

  @Test
  public void testSystemAuthentication() throws Exception {

    // Validate that the System Actor is authorized, even if there is no policy.

    ResourceSpec resourceSpec = new ResourceSpec("dataset", "urn:li:dataset:test");

    AuthorizationRequest request = new AuthorizationRequest(
        new Actor(ActorType.USER, DATAHUB_SYSTEM_CLIENT_ID).toUrnStr(),
        "EDIT_ENTITY_TAGS",
        Optional.of(resourceSpec)
    );

    assertEquals(_dataHubAuthorizer.authorize(request).getType(), AuthorizationResult.Type.ALLOW);
  }

  @Test
  public void testAuthorizeGranted() throws Exception {

    ResourceSpec resourceSpec = new ResourceSpec("dataset", "urn:li:dataset:test");

    AuthorizationRequest request = new AuthorizationRequest(
        "urn:li:corpuser:test",
        "EDIT_ENTITY_TAGS",
        Optional.of(resourceSpec)
      );

    assertEquals(_dataHubAuthorizer.authorize(request).getType(), AuthorizationResult.Type.ALLOW);
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

    assertEquals(_dataHubAuthorizer.authorize(request).getType(), AuthorizationResult.Type.DENY);
  }

  @Test
  public void testAllowAllMode() throws Exception {

    _dataHubAuthorizer.setMode(DataHubAuthorizer.AuthorizationMode.ALLOW_ALL);

    ResourceSpec resourceSpec = new ResourceSpec("dataset", "urn:li:dataset:test");

    // Policy for this privilege is inactive.
    AuthorizationRequest request = new AuthorizationRequest(
        "urn:li:corpuser:test",
        "EDIT_ENTITY_OWNERS",
        Optional.of(resourceSpec)
    );

    assertEquals(_dataHubAuthorizer.authorize(request).getType(), AuthorizationResult.Type.ALLOW);
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

    assertEquals(_dataHubAuthorizer.authorize(request).getType(), AuthorizationResult.Type.ALLOW);

    // Now init the mocks to return 0 policies.
    final SearchResult emptyResult = new SearchResult();
    emptyResult.setNumEntities(0);
    emptyResult.setEntities(new SearchEntityArray());

    when(_entityClient.search(eq("dataHubPolicy"), eq(""), isNull(), any(), anyInt(), anyInt(), any(),
        eq(new SearchFlags().setFulltext(true)))).thenReturn(emptyResult);
    when(_entityClient.batchGetV2(eq(POLICY_ENTITY_NAME), eq(Collections.emptySet()), eq(null), any())).thenReturn(
        Collections.emptyMap());

    // Invalidate Cache.
    _dataHubAuthorizer.invalidateCache();
    Thread.sleep(500); // Sleep so the runnable can execute. (not ideal)
    // Now verify that invalidating the cache updates the policies by running the same authorization request.
    assertEquals(_dataHubAuthorizer.authorize(request).getType(), AuthorizationResult.Type.DENY);
  }

  @Test
  public void testAuthorizedActorsActivePolicy() throws Exception {
    final AuthorizedActors actors =
        _dataHubAuthorizer.authorizedActors("EDIT_ENTITY_TAGS", // Should be inside the active policy.
            Optional.of(new ResourceSpec("dataset", "urn:li:dataset:1")));

    assertTrue(actors.isAllUsers());
    assertTrue(actors.isAllGroups());

    assertEquals(new HashSet<>(actors.getUsers()), ImmutableSet.of(
        Urn.createFromString("urn:li:corpuser:user1"),
        Urn.createFromString("urn:li:corpuser:user2"),
        Urn.createFromString("urn:li:corpuser:user3"),
        Urn.createFromString("urn:li:corpuser:user4")
    ));

    assertEquals(new HashSet<>(actors.getGroups()), ImmutableSet.of(
        Urn.createFromString("urn:li:corpGroup:group1"),
        Urn.createFromString("urn:li:corpGroup:group2"),
        Urn.createFromString("urn:li:corpGroup:group3"),
        Urn.createFromString("urn:li:corpGroup:group4")
    ));
  }

  @Test
  public void testAuthorizationOnDomainWithPrivilegeIsAllowed() {
    ResourceSpec resourceSpec = new ResourceSpec("dataset", "urn:li:dataset:test");

    AuthorizationRequest request = new AuthorizationRequest(
        "urn:li:corpuser:test",
        "EDIT_ENTITY_STATUS",
        Optional.of(resourceSpec)
    );

    assertEquals(_dataHubAuthorizer.authorize(request).getType(), AuthorizationResult.Type.ALLOW);
  }

  @Test
  public void testAuthorizationOnDomainWithParentPrivilegeIsAllowed() {
    ResourceSpec resourceSpec = new ResourceSpec("dataset", "urn:li:dataset:test");

    AuthorizationRequest request = new AuthorizationRequest(
        "urn:li:corpuser:test",
        "EDIT_ENTITY_DOCS",
        Optional.of(resourceSpec)
    );

    assertEquals(_dataHubAuthorizer.authorize(request).getType(), AuthorizationResult.Type.ALLOW);
  }

  @Test
  public void testAuthorizationOnDomainWithoutPrivilegeIsDenied() {
    ResourceSpec resourceSpec = new ResourceSpec("dataset", "urn:li:dataset:test");

    AuthorizationRequest request = new AuthorizationRequest(
        "urn:li:corpuser:test",
        "EDIT_ENTITY_DOC_LINKS",
        Optional.of(resourceSpec)
    );

    assertEquals(_dataHubAuthorizer.authorize(request).getType(), AuthorizationResult.Type.DENY);
  }

  private DataHubPolicyInfo createDataHubPolicyInfo(boolean active, List<String> privileges, @Nullable final Urn domain) throws Exception {
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

    if (domain != null) {
      resourceFilter.setFilter(FilterUtils.newFilter(ImmutableMap.of(ResourceFieldType.DOMAIN, Collections.singletonList(domain.toString()))));
    }

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

  private EntityResponse createDomainsResponse(final Urn domainUrn) {
    final List<Urn> domainUrns = ImmutableList.of(domainUrn);
    final EntityResponse domainsResponse = new EntityResponse();
    EnvelopedAspectMap domainsAspectMap = new EnvelopedAspectMap();
    final Domains domains = new Domains();
    domains.setDomains(new UrnArray(domainUrns));
    domainsAspectMap.put(DOMAINS_ASPECT_NAME, new EnvelopedAspect()
        .setValue(new com.linkedin.entity.Aspect(domains.data())));
    domainsResponse.setAspects(domainsAspectMap);
    return domainsResponse;
  }

  private Map<Urn, EntityResponse> createDomainPropertiesBatchResponse(@Nullable final Urn parentDomainUrn) {
    final Map<Urn, EntityResponse> batchResponse = new HashMap<>();
    final EntityResponse response = new EntityResponse();
    EnvelopedAspectMap aspectMap = new EnvelopedAspectMap();
    final DomainProperties properties = new DomainProperties();
    if (parentDomainUrn != null) {
      properties.setParentDomain(parentDomainUrn);
    }
    aspectMap.put(DOMAIN_PROPERTIES_ASPECT_NAME, new EnvelopedAspect()
        .setValue(new com.linkedin.entity.Aspect(properties.data())));
    response.setAspects(aspectMap);
    batchResponse.put(parentDomainUrn, response);
    return batchResponse;
  }

  private AuthorizerContext createAuthorizerContext(final Authentication systemAuthentication, final EntityClient entityClient) {
    return new AuthorizerContext(Collections.emptyMap(), new DefaultResourceSpecResolver(systemAuthentication, entityClient));
  }
}
