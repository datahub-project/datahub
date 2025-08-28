package com.datahub.authorization;

import static com.linkedin.metadata.Constants.*;
import static com.linkedin.metadata.authorization.PoliciesConfig.ACTIVE_POLICY_STATE;
import static com.linkedin.metadata.authorization.PoliciesConfig.INACTIVE_POLICY_STATE;
import static com.linkedin.metadata.authorization.PoliciesConfig.METADATA_POLICY_TYPE;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.*;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.Owner;
import com.linkedin.common.OwnerArray;
import com.linkedin.common.Ownership;
import com.linkedin.common.OwnershipType;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.container.Container;
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
import java.util.*;
import javax.annotation.Nullable;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class DataHubAuthorizerTest {

  public static final String DATAHUB_SYSTEM_CLIENT_ID = "__datahub_system";

  private static final Urn PARENT_DOMAIN_URN = UrnUtils.getUrn("urn:li:domain:parent");
  private static final Urn CHILD_DOMAIN_URN = UrnUtils.getUrn("urn:li:domain:child");

  private static final Urn PARENT_CONTAINER_URN = UrnUtils.getUrn("urn:li:container:parent");
  private static final Urn CHILD_CONTAINER_URN = UrnUtils.getUrn("urn:li:container:child");

  private static final Urn USER_WITH_ADMIN_ROLE =
      UrnUtils.getUrn("urn:li:corpuser:user-with-admin");
  private static final Urn USER_WITH_DOMAIN_ACCESS =
      UrnUtils.getUrn("urn:li:corpuser:domainAccessUser");
  private static final Urn USER_WITH_CONTAINER_ACCESS =
      UrnUtils.getUrn("urn:li:corpuser:containerAccessUser");
  private static final Urn USER_WITH_DOMAIN_AND_CONTAINER_ACCESS =
      UrnUtils.getUrn("urn:li:corpuser:domainAndContainerAccessUser");

  private static final Urn RESOURCE_WITH_DOMAIN = UrnUtils.getUrn("urn:li:dataset:testDomain");
  private static final Urn RESOURCE_WITH_CONTAINER =
      UrnUtils.getUrn("urn:li:dataset:testContainer");
  private static final Urn RESOURCE_WITH_DOMAIN_AND_CONTAINER =
      UrnUtils.getUrn("urn:li:dataset:testDomainContainer");

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
        createDataHubPolicyInfoFor(
            true,
            ImmutableList.of("PARENT_DOMAIN_PRIVILEGE"),
            PARENT_DOMAIN_URN,
            null,
            USER_WITH_DOMAIN_ACCESS);
    final EnvelopedAspectMap parentDomainPolicyAspectMap = new EnvelopedAspectMap();
    parentDomainPolicyAspectMap.put(
        DATAHUB_POLICY_INFO_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(parentDomainPolicy.data())));

    final Urn parentDomainPolicyCloneUrn = Urn.createFromString("urn:li:dataHubPolicy:22");

    final Urn childDomainPolicyUrn = Urn.createFromString("urn:li:dataHubPolicy:3");
    final DataHubPolicyInfo childDomainPolicy =
        createDataHubPolicyInfoFor(
            true,
            ImmutableList.of("CHILD_DOMAIN_PRIVILEGE"),
            CHILD_DOMAIN_URN,
            null,
            USER_WITH_DOMAIN_ACCESS);
    final EnvelopedAspectMap childDomainPolicyAspectMap = new EnvelopedAspectMap();
    childDomainPolicyAspectMap.put(
        DATAHUB_POLICY_INFO_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(childDomainPolicy.data())));

    final Urn parentContainerPolicyUrn = Urn.createFromString("urn:li:dataHubPolicy:4");
    final DataHubPolicyInfo parentContainerPolicy =
        createDataHubPolicyInfoFor(
            true,
            ImmutableList.of("PARENT_CONTAINER_PRIVILEGE"),
            null,
            PARENT_CONTAINER_URN,
            USER_WITH_CONTAINER_ACCESS);
    final EnvelopedAspectMap parentContainerPolicyAspectMap = new EnvelopedAspectMap();
    parentContainerPolicyAspectMap.put(
        DATAHUB_POLICY_INFO_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(parentContainerPolicy.data())));

    final Urn parentContainerPolicyCloneUrn = Urn.createFromString("urn:li:dataHubPolicy:44");

    final Urn childContainerPolicyUrn = Urn.createFromString("urn:li:dataHubPolicy:5");
    final DataHubPolicyInfo childContainerPolicy =
        createDataHubPolicyInfoFor(
            true,
            ImmutableList.of("CHILD_CONTAINER_PRIVILEGE"),
            null,
            CHILD_CONTAINER_URN,
            USER_WITH_CONTAINER_ACCESS);
    final EnvelopedAspectMap childContainerPolicyAspectMap = new EnvelopedAspectMap();
    childContainerPolicyAspectMap.put(
        DATAHUB_POLICY_INFO_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(childContainerPolicy.data())));

    final Urn domainAndContainerPolicyUrn = Urn.createFromString("urn:li:dataHubPolicy:6");
    final DataHubPolicyInfo domainAndContainerPolicy =
        createDataHubPolicyInfoFor(
            true,
            ImmutableList.of("DOMAIN_AND_CONTAINER_PRIVILEGE"),
            CHILD_DOMAIN_URN,
            CHILD_CONTAINER_URN,
            USER_WITH_DOMAIN_AND_CONTAINER_ACCESS);

    final EnvelopedAspectMap domainAndContainerPolicyAspectMap = new EnvelopedAspectMap();
    domainAndContainerPolicyAspectMap.put(
        DATAHUB_POLICY_INFO_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(domainAndContainerPolicy.data())));

    final Urn adminPolicyUrn = Urn.createFromString("urn:li:dataHubPolicy:7");
    final DataHubActorFilter actorFilter = new DataHubActorFilter();
    actorFilter.setRoles(
        new UrnArray(ImmutableList.of(Urn.createFromString("urn:li:dataHubRole:Admin"))));
    final DataHubPolicyInfo adminPolicy =
        createDataHubPolicyInfoFor(
            true, ImmutableList.of("EDIT_USER_PROFILE"), null, null, actorFilter);
    final EnvelopedAspectMap adminPolicyAspectMap = new EnvelopedAspectMap();
    adminPolicyAspectMap.put(
        DATAHUB_POLICY_INFO_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(adminPolicy.data())));

    final ScrollResult policySearchResult1 =
        new ScrollResult()
            .setScrollId("1")
            .setNumEntities(10)
            .setEntities(
                new SearchEntityArray(
                    ImmutableList.of(new SearchEntity().setEntity(activePolicyUrn))));

    final ScrollResult policySearchResult2 =
        new ScrollResult()
            .setScrollId("2")
            .setNumEntities(10)
            .setEntities(
                new SearchEntityArray(
                    ImmutableList.of(new SearchEntity().setEntity(inactivePolicyUrn))));

    final ScrollResult policySearchResult3 =
        new ScrollResult()
            .setScrollId("3")
            .setNumEntities(10)
            .setEntities(
                new SearchEntityArray(
                    ImmutableList.of(new SearchEntity().setEntity(parentDomainPolicyUrn))));

    final ScrollResult policySearchResult4 =
        new ScrollResult()
            .setScrollId("4")
            .setNumEntities(10)
            .setEntities(
                new SearchEntityArray(
                    ImmutableList.of(new SearchEntity().setEntity(childDomainPolicyUrn))));

    final ScrollResult policySearchResult5 =
        new ScrollResult()
            .setScrollId("5")
            .setNumEntities(10)
            .setEntities(
                new SearchEntityArray(
                    ImmutableList.of(new SearchEntity().setEntity(parentDomainPolicyCloneUrn))));

    final ScrollResult policySearchResult6 =
        new ScrollResult()
            .setScrollId("6")
            .setNumEntities(10)
            .setEntities(
                new SearchEntityArray(
                    ImmutableList.of(new SearchEntity().setEntity(parentContainerPolicyUrn))));

    final ScrollResult policySearchResult7 =
        new ScrollResult()
            .setScrollId("7")
            .setNumEntities(10)
            .setEntities(
                new SearchEntityArray(
                    ImmutableList.of(new SearchEntity().setEntity(childContainerPolicyUrn))));

    final ScrollResult policySearchResult8 =
        new ScrollResult()
            .setScrollId("8")
            .setNumEntities(10)
            .setEntities(
                new SearchEntityArray(
                    ImmutableList.of(new SearchEntity().setEntity(parentContainerPolicyCloneUrn))));

    final ScrollResult policySearchResult9 =
        new ScrollResult()
            .setScrollId("9")
            .setNumEntities(10)
            .setEntities(
                new SearchEntityArray(
                    ImmutableList.of(new SearchEntity().setEntity(domainAndContainerPolicyUrn))));

    final ScrollResult policySearchResult10 =
        new ScrollResult()
            .setNumEntities(10)
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
            anyList(),
            anyInt()))
        .thenReturn(policySearchResult1);
    when(_entityClient.scrollAcrossEntities(
            any(OperationContext.class),
            eq(List.of("dataHubPolicy")),
            eq(""),
            nullable(Filter.class),
            eq("1"),
            isNull(),
            anyList(),
            anyInt()))
        .thenReturn(policySearchResult2);
    when(_entityClient.scrollAcrossEntities(
            any(OperationContext.class),
            eq(List.of("dataHubPolicy")),
            eq(""),
            nullable(Filter.class),
            eq("2"),
            isNull(),
            anyList(),
            anyInt()))
        .thenReturn(policySearchResult3);
    when(_entityClient.scrollAcrossEntities(
            any(OperationContext.class),
            eq(List.of("dataHubPolicy")),
            eq(""),
            nullable(Filter.class),
            eq("3"),
            isNull(),
            anyList(),
            anyInt()))
        .thenReturn(policySearchResult4);
    when(_entityClient.scrollAcrossEntities(
            any(OperationContext.class),
            eq(List.of("dataHubPolicy")),
            eq(""),
            nullable(Filter.class),
            eq("4"),
            isNull(),
            anyList(),
            anyInt()))
        .thenReturn(policySearchResult5);
    when(_entityClient.scrollAcrossEntities(
            any(OperationContext.class),
            eq(List.of("dataHubPolicy")),
            eq(""),
            nullable(Filter.class),
            eq("5"),
            isNull(),
            anyList(),
            anyInt()))
        .thenReturn(policySearchResult6);
    when(_entityClient.scrollAcrossEntities(
            any(OperationContext.class),
            eq(List.of("dataHubPolicy")),
            eq(""),
            nullable(Filter.class),
            eq("6"),
            isNull(),
            anyList(),
            anyInt()))
        .thenReturn(policySearchResult7);
    when(_entityClient.scrollAcrossEntities(
            any(OperationContext.class),
            eq(List.of("dataHubPolicy")),
            eq(""),
            nullable(Filter.class),
            eq("7"),
            isNull(),
            anyList(),
            anyInt()))
        .thenReturn(policySearchResult8);
    when(_entityClient.scrollAcrossEntities(
            any(OperationContext.class),
            eq(List.of("dataHubPolicy")),
            eq(""),
            nullable(Filter.class),
            eq("8"),
            isNull(),
            anyList(),
            anyInt()))
        .thenReturn(policySearchResult9);
    when(_entityClient.scrollAcrossEntities(
            any(OperationContext.class),
            eq(List.of("dataHubPolicy")),
            eq(""),
            nullable(Filter.class),
            eq("9"),
            isNull(),
            anyList(),
            anyInt()))
        .thenReturn(policySearchResult10);

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
                case "urn:li:dataHubPolicy:22":
                  return Map.of(
                      parentDomainPolicyCloneUrn,
                      new EntityResponse()
                          .setUrn(parentDomainPolicyCloneUrn)
                          .setAspects(parentDomainPolicyAspectMap));
                case "urn:li:dataHubPolicy:3":
                  return Map.of(
                      childDomainPolicyUrn,
                      new EntityResponse()
                          .setUrn(childDomainPolicyUrn)
                          .setAspects(childDomainPolicyAspectMap));
                case "urn:li:dataHubPolicy:4":
                  return Map.of(
                      parentContainerPolicyUrn,
                      new EntityResponse()
                          .setUrn(parentContainerPolicyUrn)
                          .setAspects(parentContainerPolicyAspectMap));
                case "urn:li:dataHubPolicy:44":
                  return Map.of(
                      parentContainerPolicyCloneUrn,
                      new EntityResponse()
                          .setUrn(parentContainerPolicyCloneUrn)
                          .setAspects(parentContainerPolicyAspectMap));
                case "urn:li:dataHubPolicy:5":
                  return Map.of(
                      childContainerPolicyUrn,
                      new EntityResponse()
                          .setUrn(childContainerPolicyUrn)
                          .setAspects(childContainerPolicyAspectMap));
                case "urn:li:dataHubPolicy:6":
                  return Map.of(
                      domainAndContainerPolicyUrn,
                      new EntityResponse()
                          .setUrn(domainAndContainerPolicyUrn)
                          .setAspects(domainAndContainerPolicyAspectMap));
                case "urn:li:dataHubPolicy:7":
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
            argThat(Set.of(RESOURCE_WITH_DOMAIN, RESOURCE_WITH_DOMAIN_AND_CONTAINER)::contains),
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

    // Mocks to get containers on a resource
    when(_entityClient.getV2(
            any(OperationContext.class),
            any(),
            argThat(Set.of(RESOURCE_WITH_CONTAINER, RESOURCE_WITH_DOMAIN_AND_CONTAINER)::contains),
            eq(Collections.singleton(CONTAINER_ASPECT_NAME))))
        .thenReturn(createContainerResponse(CHILD_CONTAINER_URN));

    // Mocks to get parent container of a container
    when(_entityClient.getV2(
            any(OperationContext.class),
            any(),
            eq(CHILD_CONTAINER_URN),
            eq(Collections.singleton(CONTAINER_ASPECT_NAME))))
        .thenReturn(createContainerResponse(PARENT_CONTAINER_URN));

    // Mocks to reach the stopping point on container parent
    when(_entityClient.getV2(
            any(OperationContext.class),
            any(),
            eq(PARENT_CONTAINER_URN),
            eq(Collections.singleton(CONTAINER_ASPECT_NAME))))
        .thenReturn(null);

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
            null,
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
            Optional.of(resourceSpec),
            Collections.emptyList());

    assertEquals(_dataHubAuthorizer.authorize(request).getType(), AuthorizationResult.Type.ALLOW);
  }

  @Test
  public void testAuthorizeGranted() throws Exception {

    EntitySpec resourceSpec = new EntitySpec("dataset", "urn:li:dataset:test");

    AuthorizationRequest request =
        new AuthorizationRequest(
            "urn:li:corpuser:test",
            "EDIT_ENTITY_TAGS",
            Optional.of(resourceSpec),
            Collections.emptyList());

    assertEquals(_dataHubAuthorizer.authorize(request).getType(), AuthorizationResult.Type.ALLOW);
  }

  @Test
  public void testAuthorizeNotGranted() throws Exception {

    EntitySpec resourceSpec = new EntitySpec("dataset", "urn:li:dataset:test");

    // Policy for this privilege is inactive.
    AuthorizationRequest request =
        new AuthorizationRequest(
            "urn:li:corpuser:test",
            "EDIT_ENTITY_OWNERS",
            Optional.of(resourceSpec),
            Collections.emptyList());

    assertEquals(_dataHubAuthorizer.authorize(request).getType(), AuthorizationResult.Type.DENY);
  }

  @Test
  public void testAllowAllMode() throws Exception {

    _dataHubAuthorizer.setMode(DataHubAuthorizer.AuthorizationMode.ALLOW_ALL);

    EntitySpec resourceSpec = new EntitySpec("dataset", "urn:li:dataset:test");

    // Policy for this privilege is inactive.
    AuthorizationRequest request =
        new AuthorizationRequest(
            "urn:li:corpuser:test",
            "EDIT_ENTITY_OWNERS",
            Optional.of(resourceSpec),
            Collections.emptyList());

    assertEquals(_dataHubAuthorizer.authorize(request).getType(), AuthorizationResult.Type.ALLOW);
  }

  @Test
  public void testInvalidateCache() throws Exception {

    // First make sure that the default policies are as expected.
    EntitySpec resourceSpec = new EntitySpec("dataset", "urn:li:dataset:test");

    AuthorizationRequest request =
        new AuthorizationRequest(
            "urn:li:corpuser:test",
            "EDIT_ENTITY_TAGS",
            Optional.of(resourceSpec),
            Collections.emptyList());

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
            anyList(),
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
            USER_WITH_ADMIN_ROLE.toString(),
            "EDIT_USER_PROFILE",
            Optional.of(resourceSpec),
            Collections.emptyList());

    assertEquals(_dataHubAuthorizer.authorize(request).getType(), AuthorizationResult.Type.ALLOW);
  }

  @Test
  public void testAuthorizationOnDomainWithPrivilegeIsAllowed() throws Exception {
    EntitySpec resourceSpec = new EntitySpec("dataset", RESOURCE_WITH_DOMAIN.toString());

    AuthorizationRequest request =
        new AuthorizationRequest(
            USER_WITH_DOMAIN_ACCESS.toString(),
            "CHILD_DOMAIN_PRIVILEGE",
            Optional.of(resourceSpec),
            Collections.emptyList());

    assertEquals(_dataHubAuthorizer.authorize(request).getType(), AuthorizationResult.Type.ALLOW);
    verify(_entityClient, times(1))
        .getV2(
            any(OperationContext.class),
            eq("dataset"),
            eq(RESOURCE_WITH_DOMAIN),
            eq(Collections.singleton(DOMAINS_ASPECT_NAME)));
    // not checking ancestor domain resolution calls which happen through batchGetV2
  }

  @Test
  public void testAuthorizationOnDomainWithParentPrivilegeIsAllowed() throws Exception {
    EntitySpec resourceSpec = new EntitySpec("dataset", RESOURCE_WITH_DOMAIN.toString());

    AuthorizationRequest request =
        new AuthorizationRequest(
            USER_WITH_DOMAIN_ACCESS.toString(),
            "PARENT_DOMAIN_PRIVILEGE",
            Optional.of(resourceSpec),
            Collections.emptyList());

    assertEquals(_dataHubAuthorizer.authorize(request).getType(), AuthorizationResult.Type.ALLOW);
    verify(_entityClient, times(1))
        .getV2(
            any(OperationContext.class),
            eq("dataset"),
            eq(RESOURCE_WITH_DOMAIN),
            eq(Collections.singleton(DOMAINS_ASPECT_NAME)));
    // not checking ancestor domain resolution calls which happen through batchGetV2
  }

  @Test
  public void testAuthorizationOnDomainWithoutPrivilegeMatchIsDenied() throws Exception {
    EntitySpec resourceSpec = new EntitySpec("dataset", RESOURCE_WITH_DOMAIN.toString());

    AuthorizationRequest request =
        new AuthorizationRequest(
            USER_WITH_DOMAIN_ACCESS.toString(),
            "RANDOM_PRIVILEGE",
            Optional.of(resourceSpec),
            Collections.emptyList());

    assertEquals(_dataHubAuthorizer.authorize(request).getType(), AuthorizationResult.Type.DENY);

    // domain resolver should not be invoked at all
    verify(_entityClient, times(0))
        .getV2(
            any(OperationContext.class),
            eq("dataset"),
            eq(RESOURCE_WITH_DOMAIN),
            eq(Collections.singleton(DOMAINS_ASPECT_NAME)));
  }

  @Test
  public void testAuthorizationOnDomainWithoutUserMatchIsDenied() throws Exception {
    EntitySpec resourceSpec = new EntitySpec("dataset", RESOURCE_WITH_DOMAIN.toString());

    AuthorizationRequest request =
        new AuthorizationRequest(
            "urn:li:corpuser:unauthorizedUser",
            "PARENT_DOMAIN_PRIVILEGE",
            Optional.of(resourceSpec),
            Collections.emptyList());

    assertEquals(_dataHubAuthorizer.authorize(request).getType(), AuthorizationResult.Type.DENY);

    // even though two domain-based policies are applicable, the domain resolver should be invoked
    // only once
    verify(_entityClient, times(1))
        .getV2(
            any(OperationContext.class),
            eq("dataset"),
            eq(RESOURCE_WITH_DOMAIN),
            eq(Collections.singleton(DOMAINS_ASPECT_NAME)));
  }

  @Test
  public void testAuthorizationOnContainerWithPrivilegeIsAllowed() throws Exception {
    EntitySpec resourceSpec = new EntitySpec("dataset", RESOURCE_WITH_CONTAINER.toString());

    AuthorizationRequest request =
        new AuthorizationRequest(
            USER_WITH_CONTAINER_ACCESS.toString(),
            "CHILD_CONTAINER_PRIVILEGE",
            Optional.of(resourceSpec),
            Collections.emptyList());

    assertEquals(_dataHubAuthorizer.authorize(request).getType(), AuthorizationResult.Type.ALLOW);

    // one hop per container in hierarchy
    verify(_entityClient, times(1))
        .getV2(
            any(OperationContext.class),
            eq("dataset"),
            eq(RESOURCE_WITH_CONTAINER),
            eq(Collections.singleton(CONTAINER_ASPECT_NAME)));
    verify(_entityClient, times(1))
        .getV2(
            any(OperationContext.class),
            eq("container"),
            eq(CHILD_CONTAINER_URN),
            eq(Collections.singleton(CONTAINER_ASPECT_NAME)));
    verify(_entityClient, times(1))
        .getV2(
            any(OperationContext.class),
            eq("container"),
            eq(PARENT_CONTAINER_URN),
            eq(Collections.singleton(CONTAINER_ASPECT_NAME)));
  }

  @Test
  public void testAuthorizationOnContainerWithParentPrivilegeIsAllowed() throws Exception {
    EntitySpec resourceSpec = new EntitySpec("dataset", RESOURCE_WITH_CONTAINER.toString());

    AuthorizationRequest request =
        new AuthorizationRequest(
            USER_WITH_CONTAINER_ACCESS.toString(),
            "PARENT_CONTAINER_PRIVILEGE",
            Optional.of(resourceSpec),
            Collections.emptyList());

    assertEquals(_dataHubAuthorizer.authorize(request).getType(), AuthorizationResult.Type.ALLOW);

    // one hop per container in hierarchy
    verify(_entityClient, times(1))
        .getV2(
            any(OperationContext.class),
            eq("dataset"),
            eq(RESOURCE_WITH_CONTAINER),
            eq(Collections.singleton(CONTAINER_ASPECT_NAME)));
    verify(_entityClient, times(1))
        .getV2(
            any(OperationContext.class),
            eq("container"),
            eq(CHILD_CONTAINER_URN),
            eq(Collections.singleton(CONTAINER_ASPECT_NAME)));
    verify(_entityClient, times(1))
        .getV2(
            any(OperationContext.class),
            eq("container"),
            eq(PARENT_CONTAINER_URN),
            eq(Collections.singleton(CONTAINER_ASPECT_NAME)));
  }

  @Test
  public void testAuthorizationOnContainerWithoutPrivilegeMatchIsDenied() throws Exception {
    EntitySpec resourceSpec = new EntitySpec("dataset", RESOURCE_WITH_CONTAINER.toString());

    AuthorizationRequest request =
        new AuthorizationRequest(
            USER_WITH_CONTAINER_ACCESS.toString(),
            "RANDOM_PRIVILEGE",
            Optional.of(resourceSpec),
            Collections.emptyList());

    assertEquals(_dataHubAuthorizer.authorize(request).getType(), AuthorizationResult.Type.DENY);

    // container resolver should not be invoked at all
    verify(_entityClient, times(0))
        .getV2(
            any(OperationContext.class),
            eq("dataset"),
            eq(RESOURCE_WITH_CONTAINER),
            eq(Collections.singleton(CONTAINER_ASPECT_NAME)));
  }

  @Test
  public void testAuthorizationOnContainerWithoutUserMatchIsDenied() throws Exception {
    EntitySpec resourceSpec = new EntitySpec("dataset", RESOURCE_WITH_CONTAINER.toString());

    AuthorizationRequest request =
        new AuthorizationRequest(
            "urn:li:corpuser:unauthorizedUser",
            "PARENT_CONTAINER_PRIVILEGE",
            Optional.of(resourceSpec),
            Collections.emptyList());

    assertEquals(_dataHubAuthorizer.authorize(request).getType(), AuthorizationResult.Type.DENY);

    // even though two container-based policies are applicable, the container resolver should be
    // invoked only once one hop per container in hierarchy
    verify(_entityClient, times(1))
        .getV2(
            any(OperationContext.class),
            eq("dataset"),
            eq(RESOURCE_WITH_CONTAINER),
            eq(Collections.singleton(CONTAINER_ASPECT_NAME)));
    verify(_entityClient, times(1))
        .getV2(
            any(OperationContext.class),
            eq("container"),
            eq(CHILD_CONTAINER_URN),
            eq(Collections.singleton(CONTAINER_ASPECT_NAME)));
    verify(_entityClient, times(1))
        .getV2(
            any(OperationContext.class),
            eq("container"),
            eq(PARENT_CONTAINER_URN),
            eq(Collections.singleton(CONTAINER_ASPECT_NAME)));
  }

  @Test
  public void testContainerOnlyPolicyAllowsResourceWithDomainAndContainer() throws Exception {
    EntitySpec resourceSpec =
        new EntitySpec("dataset", RESOURCE_WITH_DOMAIN_AND_CONTAINER.toString());

    AuthorizationRequest request =
        new AuthorizationRequest(
            USER_WITH_CONTAINER_ACCESS.toString(),
            "CHILD_CONTAINER_PRIVILEGE",
            Optional.of(resourceSpec),
            Collections.emptyList());

    assertEquals(_dataHubAuthorizer.authorize(request).getType(), AuthorizationResult.Type.ALLOW);
  }

  @Test
  public void testDomainOnlyPolicyAllowsResourceWithDomainAndContainer() throws Exception {
    EntitySpec resourceSpec =
        new EntitySpec("dataset", RESOURCE_WITH_DOMAIN_AND_CONTAINER.toString());

    AuthorizationRequest request =
        new AuthorizationRequest(
            USER_WITH_DOMAIN_ACCESS.toString(),
            "CHILD_DOMAIN_PRIVILEGE",
            Optional.of(resourceSpec),
            Collections.emptyList());

    assertEquals(_dataHubAuthorizer.authorize(request).getType(), AuthorizationResult.Type.ALLOW);
  }

  @Test
  public void testDomainAndContainerPolicyAllows() throws Exception {
    EntitySpec resourceSpec =
        new EntitySpec("dataset", RESOURCE_WITH_DOMAIN_AND_CONTAINER.toString());

    AuthorizationRequest request =
        new AuthorizationRequest(
            USER_WITH_DOMAIN_AND_CONTAINER_ACCESS.toString(),
            "DOMAIN_AND_CONTAINER_PRIVILEGE",
            Optional.of(resourceSpec),
            Collections.emptyList());

    assertEquals(_dataHubAuthorizer.authorize(request).getType(), AuthorizationResult.Type.ALLOW);
  }

  @Test
  public void testDomainAndContainerPolicyDoesNotAllowDomainResource() throws Exception {
    EntitySpec resourceSpec = new EntitySpec("dataset", RESOURCE_WITH_DOMAIN.toString());

    AuthorizationRequest request =
        new AuthorizationRequest(
            USER_WITH_DOMAIN_AND_CONTAINER_ACCESS.toString(),
            "DOMAIN_AND_CONTAINER_PRIVILEGE",
            Optional.of(resourceSpec),
            Collections.emptyList());

    assertEquals(_dataHubAuthorizer.authorize(request).getType(), AuthorizationResult.Type.DENY);
  }

  @Test
  public void testDomainAndContainerPolicyDoesNotAllowContainerResource() throws Exception {
    EntitySpec resourceSpec = new EntitySpec("dataset", RESOURCE_WITH_CONTAINER.toString());

    AuthorizationRequest request =
        new AuthorizationRequest(
            USER_WITH_DOMAIN_AND_CONTAINER_ACCESS.toString(),
            "DOMAIN_AND_CONTAINER_PRIVILEGE",
            Optional.of(resourceSpec),
            Collections.emptyList());

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
            userUrnWithoutPermissions.toString(),
            "EDIT_USER_PROFILE",
            Optional.of(resourceSpec),
            Collections.emptyList());

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

    return createDataHubPolicyInfoFor(active, privileges, domain, null, actorFilter);
  }

  private DataHubPolicyInfo createDataHubPolicyInfoFor(
      boolean active,
      List<String> privileges,
      @Nullable final Urn domain,
      @Nullable final Urn container,
      Urn user)
      throws Exception {
    DataHubActorFilter actorFilter = new DataHubActorFilter().setUsers(new UrnArray(List.of(user)));
    return createDataHubPolicyInfoFor(active, privileges, domain, container, actorFilter);
  }

  private DataHubPolicyInfo createDataHubPolicyInfoFor(
      boolean active,
      List<String> privileges,
      @Nullable final Urn domain,
      @Nullable final Urn container,
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
    resourceFilter.setType("dataset");

    Map<EntityFieldType, List<String>> filterParams = new HashMap<>();

    if (domain == null && container == null) {
      resourceFilter.setAllResources(true);
    }

    if (domain != null) {
      filterParams.put(EntityFieldType.DOMAIN, Collections.singletonList(domain.toString()));
    }

    if (container != null) {
      filterParams.put(EntityFieldType.CONTAINER, Collections.singletonList(container.toString()));
    }
    resourceFilter.setFilter(FilterUtils.newFilter(filterParams));
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

  private EntityResponse createContainerResponse(final Urn containerUrn) {
    final EntityResponse response = new EntityResponse();
    EnvelopedAspectMap aspectMap = new EnvelopedAspectMap();
    Container container = new Container().setContainer(containerUrn);
    aspectMap.put(
        CONTAINER_ASPECT_NAME,
        new EnvelopedAspect().setValue(new com.linkedin.entity.Aspect(container.data())));
    response.setAspects(aspectMap);
    return response;
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
