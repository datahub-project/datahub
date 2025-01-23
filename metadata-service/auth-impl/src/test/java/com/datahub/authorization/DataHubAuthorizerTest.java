package com.datahub.authorization;

import static com.linkedin.metadata.Constants.*;
import static com.linkedin.metadata.authorization.PoliciesConfig.ACTIVE_POLICY_STATE;
import static com.linkedin.metadata.authorization.PoliciesConfig.INACTIVE_POLICY_STATE;
import static com.linkedin.metadata.authorization.PoliciesConfig.METADATA_POLICY_TYPE;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

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
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.StringArray;
import com.linkedin.domain.DomainProperties;
import com.linkedin.domain.Domains;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.identity.GroupMembership;
import com.linkedin.identity.RoleMembership;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.ScrollResult;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.policy.DataHubActorFilter;
import com.linkedin.policy.DataHubPolicyInfo;
import com.linkedin.policy.DataHubResourceFilter;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.OperationContextConfig;
import io.datahubproject.metadata.context.RetrieverContext;
import io.datahubproject.metadata.context.ServicesRegistryContext;
import io.datahubproject.metadata.context.ValidationContext;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nullable;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class DataHubAuthorizerTest {

  public static final String DATAHUB_SYSTEM_CLIENT_ID = "__datahub_system";

  private static final Urn PARENT_DOMAIN_URN = UrnUtils.getUrn("urn:li:domain:parent");
  private static final Urn CHILD_DOMAIN_URN = UrnUtils.getUrn("urn:li:domain:child");
  private static final Urn USER_WITH_ADMIN_ROLE =
      UrnUtils.getUrn("urn:li:corpuser:user-with-admin");

  private SystemEntityClient _entityClient;
  private DataHubAuthorizer _dataHubAuthorizer;
  private OperationContext systemOpContext;

  @BeforeMethod
  public void setupTest() throws Exception {
    _entityClient = mock(SystemEntityClient.class);

    // Init mocks.
    final Urn activePolicyUrn = Urn.createFromString("urn:li:dataHubPolicy:0");
    final DataHubPolicyInfo activePolicy =
        createDataHubPolicyInfo(true, ImmutableList.of("EDIT_ENTITY_TAGS"), null);
    final EnvelopedAspectMap activeAspectMap = new EnvelopedAspectMap();
    activeAspectMap.put(
        DATAHUB_POLICY_INFO_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(activePolicy.data())));

    final Urn inactivePolicyUrn = Urn.createFromString("urn:li:dataHubPolicy:1");
    final DataHubPolicyInfo inactivePolicy =
        createDataHubPolicyInfo(false, ImmutableList.of("EDIT_ENTITY_OWNERS"), null);
    final EnvelopedAspectMap inactiveAspectMap = new EnvelopedAspectMap();
    inactiveAspectMap.put(
        DATAHUB_POLICY_INFO_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(inactivePolicy.data())));

    final Urn parentDomainPolicyUrn = Urn.createFromString("urn:li:dataHubPolicy:2");
    final DataHubPolicyInfo parentDomainPolicy =
        createDataHubPolicyInfo(true, ImmutableList.of("EDIT_ENTITY_DOCS"), PARENT_DOMAIN_URN);
    final EnvelopedAspectMap parentDomainPolicyAspectMap = new EnvelopedAspectMap();
    parentDomainPolicyAspectMap.put(
        DATAHUB_POLICY_INFO_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(parentDomainPolicy.data())));

    final Urn childDomainPolicyUrn = Urn.createFromString("urn:li:dataHubPolicy:3");
    final DataHubPolicyInfo childDomainPolicy =
        createDataHubPolicyInfo(true, ImmutableList.of("EDIT_ENTITY_STATUS"), CHILD_DOMAIN_URN);
    final EnvelopedAspectMap childDomainPolicyAspectMap = new EnvelopedAspectMap();
    childDomainPolicyAspectMap.put(
        DATAHUB_POLICY_INFO_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(childDomainPolicy.data())));

    final Urn adminPolicyUrn = Urn.createFromString("urn:li:dataHubPolicy:4");
    final DataHubActorFilter actorFilter = new DataHubActorFilter();
    actorFilter.setRoles(
        new UrnArray(ImmutableList.of(Urn.createFromString("urn:li:dataHubRole:Admin"))));
    final DataHubPolicyInfo adminPolicy =
        createDataHubPolicyInfoFor(true, ImmutableList.of("EDIT_USER_PROFILE"), null, actorFilter);
    final EnvelopedAspectMap adminPolicyAspectMap = new EnvelopedAspectMap();
    adminPolicyAspectMap.put(
        DATAHUB_POLICY_INFO_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(adminPolicy.data())));

    final ScrollResult policySearchResult1 =
        new ScrollResult()
            .setScrollId("1")
            .setNumEntities(5)
            .setEntities(
                new SearchEntityArray(
                    ImmutableList.of(new SearchEntity().setEntity(activePolicyUrn))));

    final ScrollResult policySearchResult2 =
        new ScrollResult()
            .setScrollId("2")
            .setNumEntities(5)
            .setEntities(
                new SearchEntityArray(
                    ImmutableList.of(new SearchEntity().setEntity(inactivePolicyUrn))));

    final ScrollResult policySearchResult3 =
        new ScrollResult()
            .setScrollId("3")
            .setNumEntities(5)
            .setEntities(
                new SearchEntityArray(
                    ImmutableList.of(new SearchEntity().setEntity(parentDomainPolicyUrn))));

    final ScrollResult policySearchResult4 =
        new ScrollResult()
            .setScrollId("4")
            .setNumEntities(5)
            .setEntities(
                new SearchEntityArray(
                    ImmutableList.of(new SearchEntity().setEntity(childDomainPolicyUrn))));

    final ScrollResult policySearchResult5 =
        new ScrollResult()
            .setNumEntities(5)
            .setEntities(
                new SearchEntityArray(
                    ImmutableList.of(new SearchEntity().setEntity(adminPolicyUrn))));

    when(_entityClient.scrollAcrossEntities(
            any(OperationContext.class),
            eq(List.of("dataHubPolicy")),
            eq(""),
            nullable(Filter.class),
            isNull(),
            isNull(),
            anyInt()))
        .thenReturn(policySearchResult1);
    when(_entityClient.scrollAcrossEntities(
            any(OperationContext.class),
            eq(List.of("dataHubPolicy")),
            eq(""),
            nullable(Filter.class),
            eq("1"),
            isNull(),
            anyInt()))
        .thenReturn(policySearchResult2);
    when(_entityClient.scrollAcrossEntities(
            any(OperationContext.class),
            eq(List.of("dataHubPolicy")),
            eq(""),
            nullable(Filter.class),
            eq("2"),
            isNull(),
            anyInt()))
        .thenReturn(policySearchResult3);
    when(_entityClient.scrollAcrossEntities(
            any(OperationContext.class),
            eq(List.of("dataHubPolicy")),
            eq(""),
            nullable(Filter.class),
            eq("3"),
            isNull(),
            anyInt()))
        .thenReturn(policySearchResult4);
    when(_entityClient.scrollAcrossEntities(
            any(OperationContext.class),
            eq(List.of("dataHubPolicy")),
            eq(""),
            nullable(Filter.class),
            eq("4"),
            isNull(),
            anyInt()))
        .thenReturn(policySearchResult5);

    when(_entityClient.batchGetV2(
            any(OperationContext.class), eq(POLICY_ENTITY_NAME), any(), anySet()))
        .thenAnswer(
            args -> {
              Set<Urn> inputUrns = args.getArgument(2);
              Urn urn = inputUrns.stream().findFirst().get();

              switch (urn.toString()) {
                case "urn:li:dataHubPolicy:0":
                  return Map.of(
                      activePolicyUrn,
                      new EntityResponse().setUrn(activePolicyUrn).setAspects(activeAspectMap));
                case "urn:li:dataHubPolicy:1":
                  return Map.of(
                      inactivePolicyUrn,
                      new EntityResponse().setUrn(inactivePolicyUrn).setAspects(inactiveAspectMap));
                case "urn:li:dataHubPolicy:2":
                  return Map.of(
                      parentDomainPolicyUrn,
                      new EntityResponse()
                          .setUrn(parentDomainPolicyUrn)
                          .setAspects(parentDomainPolicyAspectMap));
                case "urn:li:dataHubPolicy:3":
                  return Map.of(
                      childDomainPolicyUrn,
                      new EntityResponse()
                          .setUrn(childDomainPolicyUrn)
                          .setAspects(childDomainPolicyAspectMap));
                case "urn:li:dataHubPolicy:4":
                  return Map.of(
                      adminPolicyUrn,
                      new EntityResponse().setUrn(adminPolicyUrn).setAspects(adminPolicyAspectMap));
                default:
                  throw new IllegalStateException();
              }
            });

    final List<Urn> userUrns =
        ImmutableList.of(
            Urn.createFromString("urn:li:corpuser:user3"),
            Urn.createFromString("urn:li:corpuser:user4"));
    final List<Urn> groupUrns =
        ImmutableList.of(
            Urn.createFromString("urn:li:corpGroup:group3"),
            Urn.createFromString("urn:li:corpGroup:group4"));
    EntityResponse ownershipResponse = new EntityResponse();
    EnvelopedAspectMap ownershipAspectMap = new EnvelopedAspectMap();
    ownershipAspectMap.put(
        OWNERSHIP_ASPECT_NAME,
        new EnvelopedAspect()
            .setValue(
                new com.linkedin.entity.Aspect(createOwnershipAspect(userUrns, groupUrns).data())));
    ownershipResponse.setAspects(ownershipAspectMap);
    when(_entityClient.getV2(
            any(OperationContext.class),
            any(),
            any(),
            eq(Collections.singleton(OWNERSHIP_ASPECT_NAME))))
        .thenReturn(ownershipResponse);

    // Mocks to get domains on a resource
    when(_entityClient.getV2(
            any(OperationContext.class),
            any(),
            any(),
            eq(Collections.singleton(DOMAINS_ASPECT_NAME))))
        .thenReturn(createDomainsResponse(CHILD_DOMAIN_URN));

    // Mocks to get parent domains on a domain
    when(_entityClient.batchGetV2(
            any(OperationContext.class),
            any(),
            eq(Collections.singleton(CHILD_DOMAIN_URN)),
            eq(Collections.singleton(DOMAIN_PROPERTIES_ASPECT_NAME))))
        .thenReturn(createDomainPropertiesBatchResponse(PARENT_DOMAIN_URN));

    // Mocks to reach the stopping point on domain parents
    when(_entityClient.batchGetV2(
            any(OperationContext.class),
            any(),
            eq(Collections.singleton(PARENT_DOMAIN_URN)),
            eq(Collections.singleton(DOMAIN_PROPERTIES_ASPECT_NAME))))
        .thenReturn(createDomainPropertiesBatchResponse(null));

    // Mocks to reach role membership for a user urn
    when(_entityClient.batchGetV2(
            any(OperationContext.class),
            any(),
            eq(Collections.singleton(USER_WITH_ADMIN_ROLE)),
            eq(
                ImmutableSet.of(
                    ROLE_MEMBERSHIP_ASPECT_NAME,
                    GROUP_MEMBERSHIP_ASPECT_NAME,
                    NATIVE_GROUP_MEMBERSHIP_ASPECT_NAME))))
        .thenReturn(
            createRoleMembershipBatchResponse(
                USER_WITH_ADMIN_ROLE, UrnUtils.getUrn("urn:li:dataHubRole:Admin")));

    final Authentication systemAuthentication =
        new Authentication(new Actor(ActorType.USER, DATAHUB_SYSTEM_CLIENT_ID), "");
    systemOpContext =
        OperationContext.asSystem(
            OperationContextConfig.builder().build(),
            systemAuthentication,
            mock(EntityRegistry.class),
            mock(ServicesRegistryContext.class),
            mock(IndexConvention.class),
            mock(RetrieverContext.class),
            mock(ValidationContext.class),
            true);

    _dataHubAuthorizer =
        new DataHubAuthorizer(
            systemOpContext,
            _entityClient,
            10,
            10,
            DataHubAuthorizer.AuthorizationMode.DEFAULT,
            1 // force pagination logic
            );
    _dataHubAuthorizer.init(
        Collections.emptyMap(), createAuthorizerContext(systemOpContext, _entityClient));
    _dataHubAuthorizer.invalidateCache();
    Thread.sleep(500); // Sleep so the runnable can execute. (not ideal)
  }

  @Test
  public void testSystemAuthentication() throws Exception {

    // Validate that the System Actor is authorized, even if there is no policy.

    EntitySpec resourceSpec = new EntitySpec("dataset", "urn:li:dataset:test");

    AuthorizationRequest request =
        new AuthorizationRequest(
            new Actor(ActorType.USER, DATAHUB_SYSTEM_CLIENT_ID).toUrnStr(),
            "EDIT_ENTITY_TAGS",
            Optional.of(resourceSpec));

    assertEquals(_dataHubAuthorizer.authorize(request).getType(), AuthorizationResult.Type.ALLOW);
  }

  @Test
  public void testAuthorizeGranted() throws Exception {

    EntitySpec resourceSpec = new EntitySpec("dataset", "urn:li:dataset:test");

    AuthorizationRequest request =
        new AuthorizationRequest(
            "urn:li:corpuser:test", "EDIT_ENTITY_TAGS", Optional.of(resourceSpec));

    assertEquals(_dataHubAuthorizer.authorize(request).getType(), AuthorizationResult.Type.ALLOW);
  }

  @Test
  public void testAuthorizeNotGranted() throws Exception {

    EntitySpec resourceSpec = new EntitySpec("dataset", "urn:li:dataset:test");

    // Policy for this privilege is inactive.
    AuthorizationRequest request =
        new AuthorizationRequest(
            "urn:li:corpuser:test", "EDIT_ENTITY_OWNERS", Optional.of(resourceSpec));

    assertEquals(_dataHubAuthorizer.authorize(request).getType(), AuthorizationResult.Type.DENY);
  }

  @Test
  public void testAllowAllMode() throws Exception {

    _dataHubAuthorizer.setMode(DataHubAuthorizer.AuthorizationMode.ALLOW_ALL);

    EntitySpec resourceSpec = new EntitySpec("dataset", "urn:li:dataset:test");

    // Policy for this privilege is inactive.
    AuthorizationRequest request =
        new AuthorizationRequest(
            "urn:li:corpuser:test", "EDIT_ENTITY_OWNERS", Optional.of(resourceSpec));

    assertEquals(_dataHubAuthorizer.authorize(request).getType(), AuthorizationResult.Type.ALLOW);
  }

  @Test
  public void testInvalidateCache() throws Exception {

    // First make sure that the default policies are as expected.
    EntitySpec resourceSpec = new EntitySpec("dataset", "urn:li:dataset:test");

    AuthorizationRequest request =
        new AuthorizationRequest(
            "urn:li:corpuser:test", "EDIT_ENTITY_TAGS", Optional.of(resourceSpec));

    assertEquals(_dataHubAuthorizer.authorize(request).getType(), AuthorizationResult.Type.ALLOW);

    // Now init the mocks to return 0 policies.
    final ScrollResult emptyResult = new ScrollResult();
    emptyResult.setNumEntities(0);
    emptyResult.setEntities(new SearchEntityArray());

    when(_entityClient.scrollAcrossEntities(
            any(OperationContext.class),
            eq(List.of("dataHubPolicy")),
            eq(""),
            isNull(),
            any(),
            any(),
            anyInt()))
        .thenReturn(emptyResult);
    when(_entityClient.batchGetV2(
            any(OperationContext.class),
            eq(POLICY_ENTITY_NAME),
            eq(Collections.emptySet()),
            eq(null)))
        .thenReturn(Collections.emptyMap());

    // Invalidate Cache.
    _dataHubAuthorizer.invalidateCache();
    Thread.sleep(500); // Sleep so the runnable can execute. (not ideal)

    // Now verify that invalidating the cache updates the policies by running the same authorization
    // request.
    assertEquals(_dataHubAuthorizer.authorize(request).getType(), AuthorizationResult.Type.DENY);
  }

  @Test
  public void testAuthorizedActorsActivePolicy() throws Exception {
    final AuthorizedActors actors =
        _dataHubAuthorizer.authorizedActors(
            "EDIT_ENTITY_TAGS", // Should be inside the active policy.
            Optional.of(new EntitySpec("dataset", "urn:li:dataset:1")));

    assertTrue(actors.isAllUsers());
    assertTrue(actors.isAllGroups());

    assertEquals(
        new HashSet<>(actors.getUsers()),
        ImmutableSet.of(
            Urn.createFromString("urn:li:corpuser:user1"),
            Urn.createFromString("urn:li:corpuser:user2"),
            Urn.createFromString("urn:li:corpuser:user3"),
            Urn.createFromString("urn:li:corpuser:user4")));

    assertEquals(
        new HashSet<>(actors.getGroups()),
        ImmutableSet.of(
            Urn.createFromString("urn:li:corpGroup:group1"),
            Urn.createFromString("urn:li:corpGroup:group2"),
            Urn.createFromString("urn:li:corpGroup:group3"),
            Urn.createFromString("urn:li:corpGroup:group4")));
  }

  @Test
  public void testAuthorizedRoleActivePolicy() throws Exception {
    final AuthorizedActors actors =
        _dataHubAuthorizer.authorizedActors(
            "EDIT_USER_PROFILE", // Should be inside the active policy.
            Optional.of(new EntitySpec("dataset", "urn:li:dataset:1")));

    assertFalse(actors.isAllUsers());
    assertFalse(actors.isAllGroups());
    assertEquals(new HashSet<>(actors.getUsers()), ImmutableSet.of());
    assertEquals(new HashSet<>(actors.getGroups()), ImmutableSet.of());
    assertEquals(
        new HashSet<>(actors.getRoles()),
        ImmutableSet.of(UrnUtils.getUrn("urn:li:dataHubRole:Admin")));
  }

  @Test
  public void testAuthorizationBasedOnRoleIsAllowed() {
    EntitySpec resourceSpec = new EntitySpec("dataset", "urn:li:dataset:test");

    AuthorizationRequest request =
        new AuthorizationRequest(
            USER_WITH_ADMIN_ROLE.toString(), "EDIT_USER_PROFILE", Optional.of(resourceSpec));

    assertEquals(_dataHubAuthorizer.authorize(request).getType(), AuthorizationResult.Type.ALLOW);
  }

  @Test
  public void testAuthorizationOnDomainWithPrivilegeIsAllowed() {
    EntitySpec resourceSpec = new EntitySpec("dataset", "urn:li:dataset:test");

    AuthorizationRequest request =
        new AuthorizationRequest(
            "urn:li:corpuser:test", "EDIT_ENTITY_STATUS", Optional.of(resourceSpec));

    assertEquals(_dataHubAuthorizer.authorize(request).getType(), AuthorizationResult.Type.ALLOW);
  }

  @Test
  public void testAuthorizationOnDomainWithParentPrivilegeIsAllowed() {
    EntitySpec resourceSpec = new EntitySpec("dataset", "urn:li:dataset:test");

    AuthorizationRequest request =
        new AuthorizationRequest(
            "urn:li:corpuser:test", "EDIT_ENTITY_DOCS", Optional.of(resourceSpec));

    assertEquals(_dataHubAuthorizer.authorize(request).getType(), AuthorizationResult.Type.ALLOW);
  }

  @Test
  public void testAuthorizationOnDomainWithoutPrivilegeIsDenied() {
    EntitySpec resourceSpec = new EntitySpec("dataset", "urn:li:dataset:test");

    AuthorizationRequest request =
        new AuthorizationRequest(
            "urn:li:corpuser:test", "EDIT_ENTITY_DOC_LINKS", Optional.of(resourceSpec));

    assertEquals(_dataHubAuthorizer.authorize(request).getType(), AuthorizationResult.Type.DENY);
  }

  @Test
  public void testAuthorizationGrantedBasedOnGroupRole() throws Exception {
    final EntitySpec resourceSpec = new EntitySpec("dataset", "urn:li:dataset:custom");

    final Urn userUrnWithoutPermissions = UrnUtils.getUrn("urn:li:corpuser:userWithoutRole");
    final Urn groupWithAdminPermission = UrnUtils.getUrn("urn:li:corpGroup:groupWithRole");
    final UrnArray groups = new UrnArray(List.of(groupWithAdminPermission));
    final GroupMembership groupMembership = new GroupMembership();
    groupMembership.setGroups(groups);

    // User has no role associated but is part of 1 group
    when(_entityClient.batchGetV2(
            any(OperationContext.class),
            any(),
            eq(Collections.singleton(userUrnWithoutPermissions)),
            eq(
                ImmutableSet.of(
                    ROLE_MEMBERSHIP_ASPECT_NAME,
                    GROUP_MEMBERSHIP_ASPECT_NAME,
                    NATIVE_GROUP_MEMBERSHIP_ASPECT_NAME))))
        .thenReturn(
            createEntityBatchResponse(
                userUrnWithoutPermissions, GROUP_MEMBERSHIP_ASPECT_NAME, groupMembership));

    // Group has a role
    when(_entityClient.batchGetV2(
            any(OperationContext.class),
            any(),
            eq(Collections.singleton(groupWithAdminPermission)),
            eq(Collections.singleton(ROLE_MEMBERSHIP_ASPECT_NAME))))
        .thenReturn(
            createRoleMembershipBatchResponse(
                groupWithAdminPermission, UrnUtils.getUrn("urn:li:dataHubRole:Admin")));

    // This request should only be valid for actor with the admin role.
    // Which the urn:li:corpuser:userWithoutRole does not have
    AuthorizationRequest request =
        new AuthorizationRequest(
            userUrnWithoutPermissions.toString(), "EDIT_USER_PROFILE", Optional.of(resourceSpec));

    assertEquals(_dataHubAuthorizer.authorize(request).getType(), AuthorizationResult.Type.ALLOW);
  }

  private DataHubPolicyInfo createDataHubPolicyInfo(
      boolean active, List<String> privileges, @Nullable final Urn domain) throws Exception {

    List<Urn> users =
        ImmutableList.of(
            Urn.createFromString("urn:li:corpuser:user1"),
            Urn.createFromString("urn:li:corpuser:user2"));
    List<Urn> groups =
        ImmutableList.of(
            Urn.createFromString("urn:li:corpGroup:group1"),
            Urn.createFromString("urn:li:corpGroup:group2"));

    final DataHubActorFilter actorFilter = new DataHubActorFilter();
    actorFilter.setResourceOwners(true);
    actorFilter.setAllUsers(true);
    actorFilter.setAllGroups(true);
    actorFilter.setUsers(new UrnArray(users));
    actorFilter.setGroups(new UrnArray(groups));

    return createDataHubPolicyInfoFor(active, privileges, domain, actorFilter);
  }

  private DataHubPolicyInfo createDataHubPolicyInfoFor(
      boolean active,
      List<String> privileges,
      @Nullable final Urn domain,
      DataHubActorFilter actorFilter)
      throws Exception {
    final DataHubPolicyInfo dataHubPolicyInfo = new DataHubPolicyInfo();
    dataHubPolicyInfo.setType(METADATA_POLICY_TYPE);
    dataHubPolicyInfo.setState(active ? ACTIVE_POLICY_STATE : INACTIVE_POLICY_STATE);
    dataHubPolicyInfo.setPrivileges(new StringArray(privileges));
    dataHubPolicyInfo.setDisplayName("My Test Display");
    dataHubPolicyInfo.setDescription("My test display!");
    dataHubPolicyInfo.setEditable(true);
    dataHubPolicyInfo.setActors(actorFilter);

    final DataHubResourceFilter resourceFilter = new DataHubResourceFilter();
    resourceFilter.setAllResources(true);
    resourceFilter.setType("dataset");

    if (domain != null) {
      resourceFilter.setFilter(
          FilterUtils.newFilter(
              ImmutableMap.of(
                  EntityFieldType.DOMAIN, Collections.singletonList(domain.toString()))));
    }

    dataHubPolicyInfo.setResources(resourceFilter);

    return dataHubPolicyInfo;
  }

  private Ownership createOwnershipAspect(final List<Urn> userOwners, final List<Urn> groupOwners)
      throws Exception {
    final Ownership ownershipAspect = new Ownership();
    final OwnerArray owners = new OwnerArray();

    if (userOwners != null) {
      userOwners.forEach(
          userUrn -> {
            final Owner userOwner = new Owner();
            userOwner.setOwner(userUrn);
            userOwner.setType(OwnershipType.DATAOWNER);
            owners.add(userOwner);
          });
    }

    if (groupOwners != null) {
      groupOwners.forEach(
          groupUrn -> {
            final Owner groupOwner = new Owner();
            groupOwner.setOwner(groupUrn);
            groupOwner.setType(OwnershipType.DATAOWNER);
            owners.add(groupOwner);
          });
    }

    ownershipAspect.setOwners(owners);
    ownershipAspect.setLastModified(
        new AuditStamp().setTime(0).setActor(Urn.createFromString("urn:li:corpuser:foo")));
    return ownershipAspect;
  }

  private EntityResponse createDomainsResponse(final Urn domainUrn) {
    final List<Urn> domainUrns = ImmutableList.of(domainUrn);
    final EntityResponse domainsResponse = new EntityResponse();
    EnvelopedAspectMap domainsAspectMap = new EnvelopedAspectMap();
    final Domains domains = new Domains();
    domains.setDomains(new UrnArray(domainUrns));
    domainsAspectMap.put(
        DOMAINS_ASPECT_NAME,
        new EnvelopedAspect().setValue(new com.linkedin.entity.Aspect(domains.data())));
    domainsResponse.setAspects(domainsAspectMap);
    return domainsResponse;
  }

  private Map<Urn, EntityResponse> createDomainPropertiesBatchResponse(
      @Nullable final Urn parentDomainUrn) {
    final Map<Urn, EntityResponse> batchResponse = new HashMap<>();
    final EntityResponse response = new EntityResponse();
    EnvelopedAspectMap aspectMap = new EnvelopedAspectMap();
    final DomainProperties properties = new DomainProperties();
    if (parentDomainUrn != null) {
      properties.setParentDomain(parentDomainUrn);
    }
    aspectMap.put(
        DOMAIN_PROPERTIES_ASPECT_NAME,
        new EnvelopedAspect().setValue(new com.linkedin.entity.Aspect(properties.data())));
    response.setAspects(aspectMap);
    batchResponse.put(parentDomainUrn, response);
    return batchResponse;
  }

  private Map<Urn, EntityResponse> createRoleMembershipBatchResponse(
      final Urn actorUrn, @Nullable final Urn roleUrn) {
    final RoleMembership membership = new RoleMembership();
    if (roleUrn != null) {
      membership.setRoles(new UrnArray(roleUrn));
    }
    return createEntityBatchResponse(actorUrn, ROLE_MEMBERSHIP_ASPECT_NAME, membership);
  }

  private Map<Urn, EntityResponse> createEntityBatchResponse(
      final Urn actorUrn, final String aspectName, final RecordTemplate aspect) {
    final Map<Urn, EntityResponse> batchResponse = new HashMap<>();
    final EntityResponse response = new EntityResponse();
    EnvelopedAspectMap aspectMap = new EnvelopedAspectMap();
    aspectMap.put(
        aspectName, new EnvelopedAspect().setValue(new com.linkedin.entity.Aspect(aspect.data())));
    response.setAspects(aspectMap);
    batchResponse.put(actorUrn, response);
    return batchResponse;
  }

  private AuthorizerContext createAuthorizerContext(
      final OperationContext systemOpContext, final SystemEntityClient entityClient) {
    return new AuthorizerContext(
        Collections.emptyMap(), new DefaultEntitySpecResolver(systemOpContext, entityClient));
  }
}
