package com.linkedin.metadata.search.utils;

import static com.linkedin.metadata.Constants.CORP_USER_ENTITY_NAME;
import static com.linkedin.metadata.Constants.DATASET_ENTITY_NAME;
import static com.linkedin.metadata.Constants.GROUP_MEMBERSHIP_ASPECT_NAME;
import static com.linkedin.metadata.Constants.OWNERSHIP_ASPECT_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.datahub.authorization.AuthorizerContext;
import com.datahub.authorization.DataHubAuthorizer;
import com.datahub.authorization.DefaultEntitySpecResolver;
import com.datahub.authorization.config.ViewAuthorizationConfiguration;
import com.datahub.plugins.auth.authorization.Authorizer;
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
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.identity.GroupMembership;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.metadata.search.MatchedFieldArray;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.search.SearchResultMetadata;
import com.linkedin.policy.DataHubActorFilter;
import com.linkedin.policy.DataHubPolicyInfo;
import com.linkedin.policy.DataHubResourceFilter;
import com.linkedin.policy.PolicyMatchCondition;
import com.linkedin.policy.PolicyMatchCriterion;
import com.linkedin.policy.PolicyMatchCriterionArray;
import com.linkedin.policy.PolicyMatchFilter;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.util.Pair;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.OperationContextConfig;
import io.datahubproject.metadata.context.RequestContext;
import io.datahubproject.metadata.context.ServicesRegistryContext;
import io.datahubproject.metadata.services.RestrictedService;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

public class ESAccessControlUtilTest {
  private static final Authentication SYSTEM_AUTH =
      new Authentication(new Actor(ActorType.USER, "SYSTEM"), "");
  private static final Urn TEST_GROUP_A = UrnUtils.getUrn("urn:li:corpGroup:a");
  private static final Urn TEST_GROUP_B = UrnUtils.getUrn("urn:li:corpGroup:b");
  private static final Urn TEST_GROUP_C = UrnUtils.getUrn("urn:li:corpGroup:c");

  // User A belongs to Groups A and C
  private static final Urn TEST_USER_A = UrnUtils.getUrn("urn:li:corpuser:a");
  private static final Urn TEST_USER_B = UrnUtils.getUrn("urn:li:corpuser:b");
  private static final Urn TECH_OWNER =
      UrnUtils.getUrn("urn:li:ownershipType:__system__technical_owner");
  private static final Urn BUS_OWNER =
      UrnUtils.getUrn("urn:li:ownershipType:__system__business_owner");
  private static final Urn DOMAIN_A = UrnUtils.getUrn("urn:li:domain:DomainA");
  private static final Urn DOMAIN_B = UrnUtils.getUrn("urn:li:domain:DomainB");
  private static final Authentication USER_A_AUTH =
      new Authentication(new Actor(ActorType.USER, TEST_USER_A.getId()), "");
  private static final Authentication USER_B_AUTH =
      new Authentication(new Actor(ActorType.USER, TEST_USER_B.getId()), "");
  private static final OperationContext ENABLED_CONTEXT =
      TestOperationContexts.systemContext(
          () ->
              OperationContextConfig.builder()
                  .allowSystemAuthentication(true)
                  .viewAuthorizationConfiguration(
                      ViewAuthorizationConfiguration.builder().enabled(true).build())
                  .build(),
          () -> SYSTEM_AUTH,
          () ->
              ServicesRegistryContext.builder().restrictedService(mockRestrictedService()).build(),
          null,
          null,
          null,
          null,
          null);

  private static final String VIEW_PRIVILEGE = "VIEW_ENTITY_PAGE";

  private static final Urn UNRESTRICTED_RESULT_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)");
  private static final Urn RESTRICTED_RESULT_URN =
      UrnUtils.getUrn("urn:li:restricted:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)");

  private static final String PREFIX_MATCH =
      "urn:li:dataset:(urn:li:dataPlatform:snowflake,long_tail_companions.adoption.human";
  private static final Urn PREFIX_MATCH_URN =
      UrnUtils.getUrn(
          "urn:li:dataset:(urn:li:dataPlatform:snowflake,long_tail_companions.adoption.humans,PROD)");
  private static final Urn PREFIX_NO_MATCH_URN =
      UrnUtils.getUrn(
          "urn:li:dataset:(urn:li:dataPlatform:snowflake,long_tail_companions.adoption.meta_humans,PROD)");
  private static final Urn RESTRICTED_PREFIX_NO_MATCH_URN =
      UrnUtils.getUrn(
          "urn:li:restricted:(urn:li:dataPlatform:snowflake,long_tail_companions.adoption.meta_humans,PROD)");

  /** Comprehensive list of policy variations */
  private static final Map<String, DataHubPolicyInfo> TEST_POLICIES =
      ImmutableMap.<String, DataHubPolicyInfo>builder()
          .put(
              "allUsers",
              new DataHubPolicyInfo()
                  .setDisplayName("")
                  .setState(PoliciesConfig.ACTIVE_POLICY_STATE)
                  .setType(PoliciesConfig.METADATA_POLICY_TYPE)
                  .setActors(
                      new DataHubActorFilter()
                          .setAllUsers(true)
                          .setGroups(new UrnArray())
                          .setUsers(new UrnArray()))
                  .setPrivileges(new StringArray(List.of(VIEW_PRIVILEGE)))
                  .setResources(
                      new DataHubResourceFilter()
                          .setFilter(
                              new PolicyMatchFilter()
                                  .setCriteria(new PolicyMatchCriterionArray()))))
          .put(
              "userA",
              new DataHubPolicyInfo()
                  .setDisplayName("")
                  .setState(PoliciesConfig.ACTIVE_POLICY_STATE)
                  .setType(PoliciesConfig.METADATA_POLICY_TYPE)
                  .setActors(
                      new DataHubActorFilter()
                          .setGroups(new UrnArray())
                          .setUsers(new UrnArray(TEST_USER_A)))
                  .setPrivileges(new StringArray(List.of(VIEW_PRIVILEGE)))
                  .setResources(
                      new DataHubResourceFilter()
                          .setFilter(
                              new PolicyMatchFilter()
                                  .setCriteria(new PolicyMatchCriterionArray()))))
          .put(
              "allGroups",
              new DataHubPolicyInfo()
                  .setDisplayName("")
                  .setState(PoliciesConfig.ACTIVE_POLICY_STATE)
                  .setType(PoliciesConfig.METADATA_POLICY_TYPE)
                  .setActors(
                      new DataHubActorFilter()
                          .setAllGroups(true)
                          .setGroups(new UrnArray())
                          .setUsers(new UrnArray()))
                  .setPrivileges(new StringArray(List.of(VIEW_PRIVILEGE)))
                  .setResources(
                      new DataHubResourceFilter()
                          .setFilter(
                              new PolicyMatchFilter()
                                  .setCriteria(new PolicyMatchCriterionArray()))))
          .put(
              "groupB",
              new DataHubPolicyInfo()
                  .setDisplayName("")
                  .setState(PoliciesConfig.ACTIVE_POLICY_STATE)
                  .setType(PoliciesConfig.METADATA_POLICY_TYPE)
                  .setActors(
                      new DataHubActorFilter()
                          .setGroups(new UrnArray(TEST_GROUP_B))
                          .setUsers(new UrnArray()))
                  .setPrivileges(new StringArray(List.of(VIEW_PRIVILEGE)))
                  .setResources(
                      new DataHubResourceFilter()
                          .setFilter(
                              new PolicyMatchFilter()
                                  .setCriteria(new PolicyMatchCriterionArray()))))
          .put(
              "groupC",
              new DataHubPolicyInfo()
                  .setDisplayName("")
                  .setState(PoliciesConfig.ACTIVE_POLICY_STATE)
                  .setType(PoliciesConfig.METADATA_POLICY_TYPE)
                  .setActors(
                      new DataHubActorFilter()
                          .setGroups(new UrnArray(TEST_GROUP_C))
                          .setUsers(new UrnArray()))
                  .setPrivileges(new StringArray(List.of(VIEW_PRIVILEGE)))
                  .setResources(
                      new DataHubResourceFilter()
                          .setFilter(
                              new PolicyMatchFilter()
                                  .setCriteria(new PolicyMatchCriterionArray()))))
          .put(
              "anyOwner",
              new DataHubPolicyInfo()
                  .setDisplayName("")
                  .setState(PoliciesConfig.ACTIVE_POLICY_STATE)
                  .setType(PoliciesConfig.METADATA_POLICY_TYPE)
                  .setActors(
                      new DataHubActorFilter()
                          .setResourceOwners(true)
                          .setGroups(new UrnArray())
                          .setUsers(new UrnArray()))
                  .setPrivileges(new StringArray(List.of(VIEW_PRIVILEGE)))
                  .setResources(
                      new DataHubResourceFilter()
                          .setFilter(
                              new PolicyMatchFilter()
                                  .setCriteria(new PolicyMatchCriterionArray()))))
          .put(
              "businessOwner",
              new DataHubPolicyInfo()
                  .setDisplayName("")
                  .setState(PoliciesConfig.ACTIVE_POLICY_STATE)
                  .setType(PoliciesConfig.METADATA_POLICY_TYPE)
                  .setActors(
                      new DataHubActorFilter()
                          .setResourceOwners(true)
                          .setResourceOwnersTypes(new UrnArray(BUS_OWNER))
                          .setGroups(new UrnArray())
                          .setUsers(new UrnArray()))
                  .setPrivileges(new StringArray(List.of(VIEW_PRIVILEGE)))
                  .setResources(
                      new DataHubResourceFilter()
                          .setFilter(
                              new PolicyMatchFilter()
                                  .setCriteria(new PolicyMatchCriterionArray()))))
          .put(
              "domainA",
              new DataHubPolicyInfo()
                  .setDisplayName("")
                  .setState(PoliciesConfig.ACTIVE_POLICY_STATE)
                  .setType(PoliciesConfig.METADATA_POLICY_TYPE)
                  .setActors(
                      new DataHubActorFilter()
                          .setAllUsers(true)
                          .setGroups(new UrnArray())
                          .setUsers(new UrnArray()))
                  .setPrivileges(new StringArray(List.of(VIEW_PRIVILEGE)))
                  .setResources(
                      new DataHubResourceFilter()
                          .setFilter(
                              new PolicyMatchFilter()
                                  .setCriteria(
                                      new PolicyMatchCriterionArray(
                                          List.of(
                                              new PolicyMatchCriterion()
                                                  .setField("DOMAIN")
                                                  .setCondition(PolicyMatchCondition.EQUALS)
                                                  .setValues(
                                                      new StringArray(
                                                          List.of(DOMAIN_A.toString())))))))))
          .put(
              "urnPrefixAllUsers",
              new DataHubPolicyInfo()
                  .setDisplayName("")
                  .setState(PoliciesConfig.ACTIVE_POLICY_STATE)
                  .setType(PoliciesConfig.METADATA_POLICY_TYPE)
                  .setActors(
                      new DataHubActorFilter()
                          .setAllUsers(true)
                          .setGroups(new UrnArray())
                          .setUsers(new UrnArray()))
                  .setPrivileges(new StringArray(List.of(VIEW_PRIVILEGE)))
                  .setResources(
                      new DataHubResourceFilter()
                          .setFilter(
                              new PolicyMatchFilter()
                                  .setCriteria(
                                      new PolicyMatchCriterionArray(
                                          List.of(
                                              new PolicyMatchCriterion()
                                                  .setField("URN")
                                                  .setCondition(PolicyMatchCondition.STARTS_WITH)
                                                  .setValues(
                                                      new StringArray(List.of(PREFIX_MATCH)))))))))
          .build();

  /** User A is a technical owner of the result User B has no ownership */
  private static final Map<Urn, Map<Urn, Set<Urn>>> TEST_OWNERSHIP =
      ImmutableMap.<Urn, Map<Urn, Set<Urn>>>builder()
          .put(UNRESTRICTED_RESULT_URN, Map.of(TEST_USER_A, Set.of(TECH_OWNER)))
          .build();

  @Test
  public void testAllUsersRestrictions() throws RemoteInvocationException, URISyntaxException {

    // USER A
    OperationContext userAContext =
        sessionWithUserAGroupAandC(List.of(TEST_POLICIES.get("allUsers")));

    SearchResult result = mockSearchResult();
    ESAccessControlUtil.restrictSearchResult(
        userAContext.withSearchFlags(flags -> flags.setIncludeRestricted(true)), result);
    assertEquals(result.getEntities().get(0).getEntity(), UNRESTRICTED_RESULT_URN);

    result = mockSearchResult();
    ESAccessControlUtil.restrictSearchResult(userAContext, result);
    assertEquals(result.getEntities().get(0).getEntity(), UNRESTRICTED_RESULT_URN);

    // USER B
    OperationContext userBContext = sessionWithUserBNoGroup(List.of(TEST_POLICIES.get("allUsers")));
    result = mockSearchResult();
    ESAccessControlUtil.restrictSearchResult(
        userBContext.withSearchFlags(flags -> flags.setIncludeRestricted(true)), result);
    assertEquals(result.getEntities().get(0).getEntity(), UNRESTRICTED_RESULT_URN);

    result = mockSearchResult();
    ESAccessControlUtil.restrictSearchResult(userBContext, result);
    assertEquals(result.getEntities().get(0).getEntity(), UNRESTRICTED_RESULT_URN);
  }

  @Test
  public void testSingeUserRestrictions() throws RemoteInvocationException, URISyntaxException {

    // USER A
    OperationContext userAContext = sessionWithUserAGroupAandC(List.of(TEST_POLICIES.get("userA")));

    SearchResult result = mockSearchResult();
    ESAccessControlUtil.restrictSearchResult(
        userAContext.withSearchFlags(flags -> flags.setIncludeRestricted(true)), result);
    assertEquals(result.getEntities().get(0).getEntity(), UNRESTRICTED_RESULT_URN);

    result = mockSearchResult();
    ESAccessControlUtil.restrictSearchResult(userAContext, result);
    assertEquals(result.getEntities().get(0).getEntity(), UNRESTRICTED_RESULT_URN);

    // USER B
    OperationContext userBContext = sessionWithUserBNoGroup(List.of(TEST_POLICIES.get("userA")));

    result = mockSearchResult();
    ESAccessControlUtil.restrictSearchResult(
        userBContext.withSearchFlags(flags -> flags.setIncludeRestricted(true)), result);
    assertEquals(
        result.getEntities().get(0).getEntity(),
        RESTRICTED_RESULT_URN,
        "Expected User B (not User A) to receive a restricted urn");
  }

  @Test
  public void testAllGroupsRestrictions() throws RemoteInvocationException, URISyntaxException {

    // USER A
    OperationContext userAContext =
        sessionWithUserAGroupAandC(List.of(TEST_POLICIES.get("allGroups")));

    SearchResult result = mockSearchResult();
    ESAccessControlUtil.restrictSearchResult(
        userAContext.withSearchFlags(flags -> flags.setIncludeRestricted(true)), result);
    assertEquals(result.getEntities().get(0).getEntity(), UNRESTRICTED_RESULT_URN);

    result = mockSearchResult();
    ESAccessControlUtil.restrictSearchResult(userAContext, result);
    assertEquals(result.getEntities().get(0).getEntity(), UNRESTRICTED_RESULT_URN);

    // USER B (No Groups!)
    OperationContext userBContext =
        sessionWithUserBNoGroup(List.of(TEST_POLICIES.get("allGroups")));

    result = mockSearchResult();
    ESAccessControlUtil.restrictSearchResult(
        userBContext.withSearchFlags(flags -> flags.setIncludeRestricted(true)), result);
    assertEquals(
        result.getEntities().get(0).getEntity(),
        RESTRICTED_RESULT_URN,
        "Expected User B (no groups) to receive a restricted urn");
  }

  @Test
  public void testSingleGroupRestrictions() throws RemoteInvocationException, URISyntaxException {

    // GROUP B Policy
    // USER A
    final OperationContext userAContext =
        sessionWithUserAGroupAandC(List.of(TEST_POLICIES.get("groupB")));

    SearchResult result = mockSearchResult();
    ESAccessControlUtil.restrictSearchResult(
        userAContext.withSearchFlags(flags -> flags.setIncludeRestricted(true)), result);
    assertEquals(
        result.getEntities().get(0).getEntity(),
        RESTRICTED_RESULT_URN,
        "Expected restricted urn because not a member of Group B");

    // USER B (No Groups!)
    final OperationContext userBContext =
        sessionWithUserBNoGroup(List.of(TEST_POLICIES.get("groupB")));

    result = mockSearchResult();
    ESAccessControlUtil.restrictSearchResult(
        userBContext.withSearchFlags(flags -> flags.setIncludeRestricted(true)), result);
    assertEquals(
        result.getEntities().get(0).getEntity(),
        RESTRICTED_RESULT_URN,
        "Expected User B (no groups) to receive a restricted urn");

    // Group C Policy
    // USER A
    final OperationContext userAGroupCContext =
        sessionWithUserAGroupAandC(List.of(TEST_POLICIES.get("groupC")));

    result = mockSearchResult();
    ESAccessControlUtil.restrictSearchResult(
        userAGroupCContext.withSearchFlags(flags -> flags.setIncludeRestricted(true)), result);
    assertEquals(result.getEntities().get(0).getEntity(), UNRESTRICTED_RESULT_URN);

    result = mockSearchResult();
    ESAccessControlUtil.restrictSearchResult(userAGroupCContext, result);
    assertEquals(result.getEntities().get(0).getEntity(), UNRESTRICTED_RESULT_URN);

    // USER B (No Groups!)
    final OperationContext userBgroupCContext =
        sessionWithUserBNoGroup(List.of(TEST_POLICIES.get("groupC")));

    result = mockSearchResult();
    ESAccessControlUtil.restrictSearchResult(
        userBgroupCContext.withSearchFlags(flags -> flags.setIncludeRestricted(true)), result);
    assertEquals(
        result.getEntities().get(0).getEntity(),
        RESTRICTED_RESULT_URN,
        "Expected User B (no groups) to receive a restricted urn");
  }

  @Test
  public void testAnyOwnerRestrictions() throws RemoteInvocationException, URISyntaxException {

    // USER A
    OperationContext userAContext =
        sessionWithUserAGroupAandC(List.of(TEST_POLICIES.get("anyOwner")));

    SearchResult result = mockSearchResult();
    ESAccessControlUtil.restrictSearchResult(
        userAContext.withSearchFlags(flags -> flags.setIncludeRestricted(true)), result);
    assertEquals(result.getEntities().get(0).getEntity(), UNRESTRICTED_RESULT_URN);

    result = mockSearchResult();
    ESAccessControlUtil.restrictSearchResult(userAContext, result);
    assertEquals(result.getEntities().get(0).getEntity(), UNRESTRICTED_RESULT_URN);

    // USER B (not an owner)
    OperationContext userBContext = sessionWithUserBNoGroup(List.of(TEST_POLICIES.get("anyOwner")));

    result = mockSearchResult();
    ESAccessControlUtil.restrictSearchResult(
        userBContext.withSearchFlags(flags -> flags.setIncludeRestricted(true)), result);
    assertEquals(
        result.getEntities().get(0).getEntity(),
        RESTRICTED_RESULT_URN,
        "Expected User B to receive a restricted urn because User B doesn't own anything");
  }

  @Test
  public void testBusinessOwnerRestrictions() throws RemoteInvocationException, URISyntaxException {

    // USER A
    final OperationContext userAContext =
        sessionWithUserAGroupAandC(List.of(TEST_POLICIES.get("businessOwner")));

    SearchResult result = mockSearchResult();
    ESAccessControlUtil.restrictSearchResult(
        userAContext.withSearchFlags(flags -> flags.setIncludeRestricted(true)), result);
    assertEquals(
        result.getEntities().get(0).getEntity(),
        RESTRICTED_RESULT_URN,
        "Expected restricted urn because not a Business Owner");

    // USER B
    final OperationContext userBContext =
        sessionWithUserBNoGroup(List.of(TEST_POLICIES.get("businessOwner")));

    result = mockSearchResult();
    ESAccessControlUtil.restrictSearchResult(
        userBContext.withSearchFlags(flags -> flags.setIncludeRestricted(true)), result);
    assertEquals(
        result.getEntities().get(0).getEntity(),
        RESTRICTED_RESULT_URN,
        "Expected User B to receive a restricted urn because not a Business Owner");
  }

  @Test
  public void testDomainRestrictions() throws RemoteInvocationException, URISyntaxException {

    // USER A
    OperationContext userAContext =
        sessionWithUserAGroupAandC(List.of(TEST_POLICIES.get("domainA")));

    SearchResult result = mockSearchResult();
    ESAccessControlUtil.restrictSearchResult(
        userAContext.withSearchFlags(flags -> flags.setIncludeRestricted(true)), result);
    assertEquals(result.getEntities().get(0).getEntity(), RESTRICTED_RESULT_URN);

    result = mockSearchResult();
    ESAccessControlUtil.restrictSearchResult(
        userAContext.withSearchFlags(flags -> flags.setIncludeRestricted(false)), result);
    assertEquals(result.getEntities().get(0).getEntity(), UNRESTRICTED_RESULT_URN);

    // USER B
    OperationContext userBContext = sessionWithUserBNoGroup(List.of(TEST_POLICIES.get("domainA")));
    result = mockSearchResult();
    ESAccessControlUtil.restrictSearchResult(
        userBContext.withSearchFlags(flags -> flags.setIncludeRestricted(true)), result);
    assertEquals(result.getEntities().get(0).getEntity(), RESTRICTED_RESULT_URN);

    result = mockSearchResult();
    ESAccessControlUtil.restrictSearchResult(
        userBContext.withSearchFlags(flags -> flags.setIncludeRestricted(false)), result);
    assertEquals(result.getEntities().get(0).getEntity(), UNRESTRICTED_RESULT_URN);
  }

  @Test
  public void testPrefixRestrictions() throws RemoteInvocationException, URISyntaxException {

    // USER A
    OperationContext userAContext =
        sessionWithUserAGroupAandC(List.of(TEST_POLICIES.get("urnPrefixAllUsers")));

    SearchResult result = mockPrefixSearchResult();
    ESAccessControlUtil.restrictSearchResult(
        userAContext.withSearchFlags(flags -> flags.setIncludeRestricted(true)), result);
    assertEquals(result.getEntities().size(), 2);
    assertEquals(result.getEntities().get(0).getEntity(), PREFIX_MATCH_URN);
    assertEquals(result.getEntities().get(1).getEntity(), RESTRICTED_PREFIX_NO_MATCH_URN);

    result = mockPrefixSearchResult();
    ESAccessControlUtil.restrictSearchResult(
        userAContext.withSearchFlags(flags -> flags.setIncludeRestricted(false)), result);
    assertEquals(result.getEntities().size(), 2);
    assertEquals(result.getEntities().get(0).getEntity(), PREFIX_MATCH_URN);
    assertEquals(result.getEntities().get(1).getEntity(), PREFIX_NO_MATCH_URN);
  }

  private static RestrictedService mockRestrictedService() {
    RestrictedService mockRestrictedService = mock(RestrictedService.class);
    when(mockRestrictedService.encryptRestrictedUrn(any()))
        .thenAnswer(
            args -> {
              Urn urn = args.getArgument(0);
              return UrnUtils.getUrn(urn.toString().replace("urn:li:dataset", "urn:li:restricted"));
            });
    return mockRestrictedService;
  }

  private static SearchResult mockPrefixSearchResult() {
    SearchResult result = new SearchResult();
    result.setFrom(0);
    result.setPageSize(10);
    result.setNumEntities(1);
    result.setEntities(
        new SearchEntityArray(
            new SearchEntity()
                .setEntity(PREFIX_MATCH_URN)
                .setMatchedFields(new MatchedFieldArray()),
            new SearchEntity()
                .setEntity(PREFIX_NO_MATCH_URN)
                .setMatchedFields(new MatchedFieldArray())));
    result.setMetadata(mock(SearchResultMetadata.class));
    return result;
  }

  private static SearchResult mockSearchResult() {
    SearchResult result = new SearchResult();
    result.setFrom(0);
    result.setPageSize(10);
    result.setNumEntities(1);
    result.setEntities(
        new SearchEntityArray(
            new SearchEntity()
                .setEntity(UNRESTRICTED_RESULT_URN)
                .setMatchedFields(new MatchedFieldArray())));
    result.setMetadata(mock(SearchResultMetadata.class));
    return result;
  }

  private static OperationContext sessionWithUserAGroupAandC(List<DataHubPolicyInfo> policies)
      throws RemoteInvocationException, URISyntaxException {
    return sessionWithUserGroups(USER_A_AUTH, policies, List.of(TEST_GROUP_A, TEST_GROUP_C));
  }

  private static OperationContext sessionWithUserBNoGroup(List<DataHubPolicyInfo> policies)
      throws RemoteInvocationException, URISyntaxException {
    return sessionWithUserGroups(USER_B_AUTH, policies, List.of());
  }

  private static OperationContext sessionWithUserGroups(
      Authentication auth, List<DataHubPolicyInfo> policies, List<Urn> groups)
      throws RemoteInvocationException, URISyntaxException {
    Urn actorUrn = UrnUtils.getUrn(auth.getActor().toUrnStr());
    Authorizer dataHubAuthorizer =
        new TestDataHubAuthorizer(
            ENABLED_CONTEXT, policies, Map.of(actorUrn, groups), TEST_OWNERSHIP);
    return ENABLED_CONTEXT.asSession(RequestContext.TEST, dataHubAuthorizer, auth);
  }

  public static class TestDataHubAuthorizer extends DataHubAuthorizer {

    public TestDataHubAuthorizer(
        @Nonnull OperationContext opContext,
        @Nonnull List<DataHubPolicyInfo> policies,
        @Nonnull Map<Urn, List<Urn>> userGroups,
        @Nonnull Map<Urn, Map<Urn, Set<Urn>>> resourceOwnerTypes)
        throws RemoteInvocationException, URISyntaxException {
      super(
          ENABLED_CONTEXT,
          mockUserGroupEntityClient(userGroups, resourceOwnerTypes),
          0,
          0,
          AuthorizationMode.DEFAULT,
          0);

      DefaultEntitySpecResolver specResolver =
          new DefaultEntitySpecResolver(
              opContext, mockUserGroupEntityClient(userGroups, resourceOwnerTypes));

      AuthorizerContext ctx = mock(AuthorizerContext.class);
      when(ctx.getEntitySpecResolver()).thenReturn(specResolver);
      init(Map.of(), ctx);

      readWriteLock.writeLock().lock();
      try {
        policyCache.clear();
        Map<String, List<DataHubPolicyInfo>> byPrivilegeName =
            policies.stream()
                .flatMap(
                    policy -> policy.getPrivileges().stream().map(priv -> Pair.of(priv, policy)))
                .collect(
                    Collectors.groupingBy(
                        Pair::getKey, Collectors.mapping(Pair::getValue, Collectors.toList())));
        policyCache.putAll(byPrivilegeName);
        policyCache.put(ALL, policies);
      } finally {
        readWriteLock.writeLock().unlock();
      }
    }

    private static SystemEntityClient mockUserGroupEntityClient(
        @Nonnull Map<Urn, List<Urn>> userGroups,
        @Nonnull Map<Urn, Map<Urn, Set<Urn>>> resourceOwnerTypes)
        throws RemoteInvocationException, URISyntaxException {
      SystemEntityClient mockEntityClient = mock(SystemEntityClient.class);
      when(mockEntityClient.batchGetV2(
              any(OperationContext.class), anyString(), anySet(), anySet()))
          .thenAnswer(
              args -> {
                String entityType = args.getArgument(1);
                Set<Urn> urns = args.getArgument(2);
                Set<String> aspectNames = args.getArgument(3);

                switch (entityType) {
                  case CORP_USER_ENTITY_NAME:
                    if (aspectNames.contains(GROUP_MEMBERSHIP_ASPECT_NAME)) {
                      return urns.stream()
                          .filter(userGroups::containsKey)
                          .map(
                              urn ->
                                  Pair.of(
                                      urn,
                                      new EntityResponse()
                                          .setUrn(urn)
                                          .setEntityName(entityType)
                                          .setAspects(
                                              new EnvelopedAspectMap(
                                                  new EnvelopedAspectMap(
                                                      Map.of(
                                                          GROUP_MEMBERSHIP_ASPECT_NAME,
                                                          new EnvelopedAspect()
                                                              .setName(GROUP_MEMBERSHIP_ASPECT_NAME)
                                                              .setValue(
                                                                  new Aspect(
                                                                      new GroupMembership()
                                                                          .setGroups(
                                                                              new UrnArray(
                                                                                  userGroups.get(
                                                                                      urn)))
                                                                          .data()))))))))
                          .collect(Collectors.toMap(Pair::getKey, Pair::getValue));
                    }
                    return Map.of();
                  case DATASET_ENTITY_NAME:
                    if (aspectNames.contains(OWNERSHIP_ASPECT_NAME)) {
                      return urns.stream()
                          .filter(resourceOwnerTypes::containsKey)
                          .map(
                              urn ->
                                  Pair.of(
                                      urn,
                                      new EntityResponse()
                                          .setUrn(urn)
                                          .setEntityName(entityType)
                                          .setAspects(
                                              new EnvelopedAspectMap(
                                                  new EnvelopedAspectMap(
                                                      Map.of(
                                                          OWNERSHIP_ASPECT_NAME,
                                                          new EnvelopedAspect()
                                                              .setName(OWNERSHIP_ASPECT_NAME)
                                                              .setValue(
                                                                  new Aspect(
                                                                      new Ownership()
                                                                          .setOwners(
                                                                              new OwnerArray(
                                                                                  resourceOwnerTypes
                                                                                      .get(urn)
                                                                                      .keySet()
                                                                                      .stream()
                                                                                      .flatMap(
                                                                                          ownerUrn ->
                                                                                              resourceOwnerTypes
                                                                                                  .get(
                                                                                                      urn)
                                                                                                  .get(
                                                                                                      ownerUrn)
                                                                                                  .stream()
                                                                                                  .map(
                                                                                                      ownerTypeUrn ->
                                                                                                          new Owner()
                                                                                                              .setTypeUrn(
                                                                                                                  ownerTypeUrn)
                                                                                                              .setOwner(
                                                                                                                  ownerUrn)
                                                                                                              .setType(
                                                                                                                  OwnershipType
                                                                                                                      .CUSTOM)))
                                                                                      .collect(
                                                                                          Collectors
                                                                                              .toSet())))
                                                                          .data()))))))))
                          .collect(Collectors.toMap(Pair::getKey, Pair::getValue));
                    }
                    return Map.of();
                  default:
                    return Map.of();
                }
              });

      // call batch interface above
      when(mockEntityClient.getV2(
              any(OperationContext.class), anyString(), any(Urn.class), anySet()))
          .thenAnswer(
              args -> {
                Urn entityUrn = args.getArgument(2);
                Map<Urn, EntityResponse> batchResponse =
                    mockEntityClient.batchGetV2(
                        args.getArgument(0),
                        entityUrn.getEntityType(),
                        Set.of(entityUrn),
                        args.getArgument(3));
                return batchResponse.get(entityUrn);
              });
      return mockEntityClient;
    }
  }
}
