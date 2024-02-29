package com.linkedin.metadata.search.utils;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.datahub.authorization.config.SearchAuthorizationConfiguration;
import com.datahub.plugins.auth.authorization.Authorizer;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringArray;
import com.linkedin.metadata.aspect.hooks.OwnerTypeMap;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.metadata.entity.TestEntityRegistry;
import com.linkedin.metadata.utils.elasticsearch.IndexConventionImpl;
import com.linkedin.policy.DataHubActorFilter;
import com.linkedin.policy.DataHubPolicyInfo;
import com.linkedin.policy.DataHubResourceFilter;
import com.linkedin.policy.PolicyMatchCondition;
import com.linkedin.policy.PolicyMatchCriterion;
import com.linkedin.policy.PolicyMatchCriterionArray;
import com.linkedin.policy.PolicyMatchFilter;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.OperationContextConfig;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.opensearch.index.query.QueryBuilders;
import org.testng.annotations.Test;

public class ESAccessControlUtilTest {
  private static final Authentication SYSTEM_AUTH =
      new Authentication(new Actor(ActorType.USER, "SYSTEM"), "");
  private static final Urn TEST_GROUP_A = UrnUtils.getUrn("urn:li:corpGroup:a");
  private static final Urn TEST_GROUP_B = UrnUtils.getUrn("urn:li:corpGroup:b");
  private static final Urn TEST_GROUP_C = UrnUtils.getUrn("urn:li:corpGroup:c");
  private static final Urn TEST_USER_A = UrnUtils.getUrn("urn:li:corpUser:a");
  private static final Urn TEST_USER_B = UrnUtils.getUrn("urn:li:corpUser:b");
  private static final Urn TECH_OWNER =
      UrnUtils.getUrn("urn:li:ownershipType:__system__technical_owner");
  private static final Urn BUS_OWNER =
      UrnUtils.getUrn("urn:li:ownershipType:__system__business_owner");
  private static final Authentication USER_AUTH =
      new Authentication(new Actor(ActorType.USER, TEST_USER_A.getId()), "");
  private static final OperationContext ENABLED_CONTEXT =
      OperationContext.asSystem(
          OperationContextConfig.builder()
              .allowSystemAuthentication(true)
              .searchAuthorizationConfiguration(
                  SearchAuthorizationConfiguration.builder().enabled(true).build())
              .build(),
          new TestEntityRegistry(),
          SYSTEM_AUTH,
          IndexConventionImpl.NO_PREFIX);

  @Test
  public void testAllUserAllGroup() {
    OperationContext allUsers =
        sessionWithPolicy(
            Set.of(
                new DataHubPolicyInfo()
                    .setState(PoliciesConfig.ACTIVE_POLICY_STATE)
                    .setType(PoliciesConfig.METADATA_POLICY_TYPE)
                    .setActors(new DataHubActorFilter().setAllUsers(true))
                    .setPrivileges(
                        new StringArray(List.of(PoliciesConfig.VIEW_ENTITY_PRIVILEGE.getType())))));
    OperationContext allGroups =
        sessionWithPolicy(
            Set.of(
                new DataHubPolicyInfo()
                    .setState(PoliciesConfig.ACTIVE_POLICY_STATE)
                    .setType(PoliciesConfig.METADATA_POLICY_TYPE)
                    .setActors(new DataHubActorFilter().setAllGroups(true))
                    .setPrivileges(
                        new StringArray(List.of(PoliciesConfig.VIEW_ENTITY_PRIVILEGE.getType())))));

    assertEquals(
        ESAccessControlUtil.buildAccessControlFilters(allUsers),
        Optional.empty(),
        "Expected no ES filters for all user access without resource restrictions");
    assertEquals(
        ESAccessControlUtil.buildAccessControlFilters(allGroups),
        Optional.empty(),
        "Expected no ES filters for all user access without resource restrictions");
  }

  @Test
  public void testAllUserAllGroupEntityType() {
    OperationContext resourceAllUsersPolicy =
        sessionWithPolicy(
            Set.of(
                new DataHubPolicyInfo()
                    .setState(PoliciesConfig.ACTIVE_POLICY_STATE)
                    .setType(PoliciesConfig.METADATA_POLICY_TYPE)
                    .setActors(new DataHubActorFilter().setAllUsers(true))
                    .setResources(
                        new DataHubResourceFilter()
                            .setFilter(
                                new PolicyMatchFilter()
                                    .setCriteria(
                                        new PolicyMatchCriterionArray(
                                            new PolicyMatchCriterion()
                                                .setField("TYPE")
                                                .setCondition(PolicyMatchCondition.EQUALS)
                                                .setValues(new StringArray("dataset", "chart"))))))
                    .setPrivileges(
                        new StringArray(List.of(PoliciesConfig.VIEW_ENTITY_PRIVILEGE.getType())))));
    OperationContext resourceAllGroupsPolicy =
        sessionWithPolicy(
            Set.of(
                new DataHubPolicyInfo()
                    .setState(PoliciesConfig.ACTIVE_POLICY_STATE)
                    .setType(PoliciesConfig.METADATA_POLICY_TYPE)
                    .setActors(new DataHubActorFilter().setAllGroups(true))
                    .setResources(
                        new DataHubResourceFilter()
                            .setFilter(
                                new PolicyMatchFilter()
                                    .setCriteria(
                                        new PolicyMatchCriterionArray(
                                            new PolicyMatchCriterion()
                                                .setField("TYPE")
                                                .setCondition(PolicyMatchCondition.EQUALS)
                                                .setValues(new StringArray("dataset", "chart"))))))
                    .setPrivileges(
                        new StringArray(List.of(PoliciesConfig.VIEW_ENTITY_PRIVILEGE.getType())))));

    assertEquals(
        ESAccessControlUtil.buildAccessControlFilters(resourceAllUsersPolicy),
        Optional.of(
            QueryBuilders.boolQuery()
                .should(
                    QueryBuilders.boolQuery()
                        .filter(
                            QueryBuilders.termsQuery(
                                "_index", List.of("datasetindex_v2", "chartindex_v2"))))
                .minimumShouldMatch(1)),
        "Expected index filter for each entity");

    assertEquals(
        ESAccessControlUtil.buildAccessControlFilters(resourceAllGroupsPolicy),
        Optional.of(
            QueryBuilders.boolQuery()
                .should(
                    QueryBuilders.boolQuery()
                        .filter(
                            QueryBuilders.termsQuery(
                                "_index", List.of("datasetindex_v2", "chartindex_v2"))))
                .minimumShouldMatch(1)),
        "Expected index filter for each entity");
  }

  @Test
  public void testAllUserAllGroupUrn() {
    OperationContext resourceAllUsersPolicy =
        sessionWithPolicy(
            Set.of(
                new DataHubPolicyInfo()
                    .setState(PoliciesConfig.ACTIVE_POLICY_STATE)
                    .setType(PoliciesConfig.METADATA_POLICY_TYPE)
                    .setActors(new DataHubActorFilter().setAllUsers(true))
                    .setResources(
                        new DataHubResourceFilter()
                            .setFilter(
                                new PolicyMatchFilter()
                                    .setCriteria(
                                        new PolicyMatchCriterionArray(
                                            new PolicyMatchCriterion()
                                                .setField("URN")
                                                .setCondition(PolicyMatchCondition.EQUALS)
                                                .setValues(
                                                    new StringArray(
                                                        "urn:li:dataset:(urn:li:dataPlatform:snowflake,long_tail_companions.analytics.ShelterDogs,PROD)",
                                                        "urn:li:dataset:(urn:li:dataPlatform:snowflake,long_tail_companions.ecommerce.account,PROD)"))))))
                    .setPrivileges(
                        new StringArray(List.of(PoliciesConfig.VIEW_ENTITY_PRIVILEGE.getType())))));
    OperationContext resourceAllGroupsPolicy =
        sessionWithPolicy(
            Set.of(
                new DataHubPolicyInfo()
                    .setState(PoliciesConfig.ACTIVE_POLICY_STATE)
                    .setType(PoliciesConfig.METADATA_POLICY_TYPE)
                    .setActors(new DataHubActorFilter().setAllGroups(true))
                    .setResources(
                        new DataHubResourceFilter()
                            .setFilter(
                                new PolicyMatchFilter()
                                    .setCriteria(
                                        new PolicyMatchCriterionArray(
                                            new PolicyMatchCriterion()
                                                .setField("URN")
                                                .setCondition(PolicyMatchCondition.EQUALS)
                                                .setValues(
                                                    new StringArray(
                                                        "urn:li:dataset:(urn:li:dataPlatform:snowflake,long_tail_companions.analytics.ShelterDogs,PROD)",
                                                        "urn:li:dataset:(urn:li:dataPlatform:snowflake,long_tail_companions.ecommerce.account,PROD)"))))))
                    .setPrivileges(
                        new StringArray(List.of(PoliciesConfig.VIEW_ENTITY_PRIVILEGE.getType())))));

    assertEquals(
        ESAccessControlUtil.buildAccessControlFilters(resourceAllUsersPolicy),
        Optional.of(
            QueryBuilders.boolQuery()
                .should(
                    QueryBuilders.boolQuery()
                        .filter(
                            QueryBuilders.termsQuery(
                                "urn",
                                List.of(
                                    "urn:li:dataset:(urn:li:dataPlatform:snowflake,long_tail_companions.analytics.ShelterDogs,PROD)",
                                    "urn:li:dataset:(urn:li:dataPlatform:snowflake,long_tail_companions.ecommerce.account,PROD)"))))
                .minimumShouldMatch(1)),
        "Expected filter for each urn");

    assertEquals(
        ESAccessControlUtil.buildAccessControlFilters(resourceAllGroupsPolicy),
        Optional.of(
            QueryBuilders.boolQuery()
                .should(
                    QueryBuilders.boolQuery()
                        .filter(
                            QueryBuilders.termsQuery(
                                "urn",
                                List.of(
                                    "urn:li:dataset:(urn:li:dataPlatform:snowflake,long_tail_companions.analytics.ShelterDogs,PROD)",
                                    "urn:li:dataset:(urn:li:dataPlatform:snowflake,long_tail_companions.ecommerce.account,PROD)"))))
                .minimumShouldMatch(1)),
        "Expected filter for each urn");
  }

  @Test
  public void testAllUserAllGroupTag() {
    OperationContext resourceAllUsersPolicy =
        sessionWithPolicy(
            Set.of(
                new DataHubPolicyInfo()
                    .setState(PoliciesConfig.ACTIVE_POLICY_STATE)
                    .setType(PoliciesConfig.METADATA_POLICY_TYPE)
                    .setActors(new DataHubActorFilter().setAllUsers(true))
                    .setResources(
                        new DataHubResourceFilter()
                            .setFilter(
                                new PolicyMatchFilter()
                                    .setCriteria(
                                        new PolicyMatchCriterionArray(
                                            new PolicyMatchCriterion()
                                                .setField("TAG")
                                                .setCondition(PolicyMatchCondition.EQUALS)
                                                .setValues(
                                                    new StringArray(
                                                        "urn:li:tag:pii", "urn:li:tag:prod"))))))
                    .setPrivileges(
                        new StringArray(List.of(PoliciesConfig.VIEW_ENTITY_PRIVILEGE.getType())))));
    OperationContext resourceAllGroupsPolicy =
        sessionWithPolicy(
            Set.of(
                new DataHubPolicyInfo()
                    .setState(PoliciesConfig.ACTIVE_POLICY_STATE)
                    .setType(PoliciesConfig.METADATA_POLICY_TYPE)
                    .setActors(new DataHubActorFilter().setAllGroups(true))
                    .setResources(
                        new DataHubResourceFilter()
                            .setFilter(
                                new PolicyMatchFilter()
                                    .setCriteria(
                                        new PolicyMatchCriterionArray(
                                            new PolicyMatchCriterion()
                                                .setField("TAG")
                                                .setCondition(PolicyMatchCondition.EQUALS)
                                                .setValues(
                                                    new StringArray(
                                                        "urn:li:tag:pii", "urn:li:tag:prod"))))))
                    .setPrivileges(
                        new StringArray(List.of(PoliciesConfig.VIEW_ENTITY_PRIVILEGE.getType())))));

    assertEquals(
        ESAccessControlUtil.buildAccessControlFilters(resourceAllUsersPolicy),
        Optional.of(
            QueryBuilders.boolQuery()
                .should(
                    QueryBuilders.boolQuery()
                        .filter(
                            QueryBuilders.termsQuery(
                                "tags.keyword", List.of("urn:li:tag:pii", "urn:li:tag:prod"))))
                .minimumShouldMatch(1)),
        "Expected filter each tag");

    assertEquals(
        ESAccessControlUtil.buildAccessControlFilters(resourceAllGroupsPolicy),
        Optional.of(
            QueryBuilders.boolQuery()
                .should(
                    QueryBuilders.boolQuery()
                        .filter(
                            QueryBuilders.termsQuery(
                                "tags.keyword", List.of("urn:li:tag:pii", "urn:li:tag:prod"))))
                .minimumShouldMatch(1)),
        "Expected filter each tag");
  }

  @Test
  public void testAllUserAllGroupDomain() {
    OperationContext resourceAllUsersPolicy =
        sessionWithPolicy(
            Set.of(
                new DataHubPolicyInfo()
                    .setState(PoliciesConfig.ACTIVE_POLICY_STATE)
                    .setType(PoliciesConfig.METADATA_POLICY_TYPE)
                    .setActors(new DataHubActorFilter().setAllUsers(true))
                    .setResources(
                        new DataHubResourceFilter()
                            .setFilter(
                                new PolicyMatchFilter()
                                    .setCriteria(
                                        new PolicyMatchCriterionArray(
                                            new PolicyMatchCriterion()
                                                .setField("DOMAIN")
                                                .setCondition(PolicyMatchCondition.EQUALS)
                                                .setValues(
                                                    new StringArray(
                                                        "urn:li:domain:f9229a0b-c5ad-47e7-9ff3-f4248c5cb634",
                                                        "urn:li:domain:7d64d0fa-66c3-445c-83db-3a324723daf8"))))))
                    .setPrivileges(
                        new StringArray(List.of(PoliciesConfig.VIEW_ENTITY_PRIVILEGE.getType())))));
    OperationContext resourceAllGroupsPolicy =
        sessionWithPolicy(
            Set.of(
                new DataHubPolicyInfo()
                    .setState(PoliciesConfig.ACTIVE_POLICY_STATE)
                    .setType(PoliciesConfig.METADATA_POLICY_TYPE)
                    .setActors(new DataHubActorFilter().setAllGroups(true))
                    .setResources(
                        new DataHubResourceFilter()
                            .setFilter(
                                new PolicyMatchFilter()
                                    .setCriteria(
                                        new PolicyMatchCriterionArray(
                                            new PolicyMatchCriterion()
                                                .setField("DOMAIN")
                                                .setCondition(PolicyMatchCondition.EQUALS)
                                                .setValues(
                                                    new StringArray(
                                                        "urn:li:domain:f9229a0b-c5ad-47e7-9ff3-f4248c5cb634",
                                                        "urn:li:domain:7d64d0fa-66c3-445c-83db-3a324723daf8"))))))
                    .setPrivileges(
                        new StringArray(List.of(PoliciesConfig.VIEW_ENTITY_PRIVILEGE.getType())))));

    assertEquals(
        ESAccessControlUtil.buildAccessControlFilters(resourceAllUsersPolicy),
        Optional.of(
            QueryBuilders.boolQuery()
                .should(
                    QueryBuilders.boolQuery()
                        .filter(
                            QueryBuilders.termsQuery(
                                "domains.keyword",
                                List.of(
                                    "urn:li:domain:f9229a0b-c5ad-47e7-9ff3-f4248c5cb634",
                                    "urn:li:domain:7d64d0fa-66c3-445c-83db-3a324723daf8"))))
                .minimumShouldMatch(1)),
        "Expected filter each domain");

    assertEquals(
        ESAccessControlUtil.buildAccessControlFilters(resourceAllGroupsPolicy),
        Optional.of(
            QueryBuilders.boolQuery()
                .should(
                    QueryBuilders.boolQuery()
                        .filter(
                            QueryBuilders.termsQuery(
                                "domains.keyword",
                                List.of(
                                    "urn:li:domain:f9229a0b-c5ad-47e7-9ff3-f4248c5cb634",
                                    "urn:li:domain:7d64d0fa-66c3-445c-83db-3a324723daf8"))))
                .minimumShouldMatch(1)),
        "Expected filter each domain");
  }

  @Test
  public void testAllUserAllGroupUnknownField() {
    OperationContext resourceAllUsersPolicy =
        sessionWithPolicy(
            Set.of(
                new DataHubPolicyInfo()
                    .setState(PoliciesConfig.ACTIVE_POLICY_STATE)
                    .setType(PoliciesConfig.METADATA_POLICY_TYPE)
                    .setActors(new DataHubActorFilter().setAllUsers(true))
                    .setResources(
                        new DataHubResourceFilter()
                            .setFilter(
                                new PolicyMatchFilter()
                                    .setCriteria(
                                        new PolicyMatchCriterionArray(
                                            new PolicyMatchCriterion()
                                                .setField("UNKNOWN FIELD")
                                                .setCondition(PolicyMatchCondition.EQUALS)
                                                .setValues(new StringArray("dataset", "chart"))))))
                    .setPrivileges(
                        new StringArray(List.of(PoliciesConfig.VIEW_ENTITY_PRIVILEGE.getType())))));
    OperationContext resourceAllGroupsPolicy =
        sessionWithPolicy(
            Set.of(
                new DataHubPolicyInfo()
                    .setState(PoliciesConfig.ACTIVE_POLICY_STATE)
                    .setType(PoliciesConfig.METADATA_POLICY_TYPE)
                    .setActors(new DataHubActorFilter().setAllGroups(true))
                    .setResources(
                        new DataHubResourceFilter()
                            .setFilter(
                                new PolicyMatchFilter()
                                    .setCriteria(
                                        new PolicyMatchCriterionArray(
                                            new PolicyMatchCriterion()
                                                .setField("UNKNOWN FIELD")
                                                .setCondition(PolicyMatchCondition.EQUALS)
                                                .setValues(new StringArray("dataset", "chart"))))))
                    .setPrivileges(
                        new StringArray(List.of(PoliciesConfig.VIEW_ENTITY_PRIVILEGE.getType())))));

    assertEquals(
        ESAccessControlUtil.buildAccessControlFilters(resourceAllUsersPolicy),
        Optional.of(QueryBuilders.boolQuery().mustNot(QueryBuilders.matchAllQuery())),
        "Expected match-none query when an unknown field is encountered");

    assertEquals(
        ESAccessControlUtil.buildAccessControlFilters(resourceAllGroupsPolicy),
        Optional.of(QueryBuilders.boolQuery().mustNot(QueryBuilders.matchAllQuery())),
        "Expected match-none query when an unknown field is encountered");
  }

  @Test
  public void testUserGroupOwner() {
    OperationContext ownerNoGroupsNoType =
        sessionWithPolicy(
            Set.of(
                new DataHubPolicyInfo()
                    .setState(PoliciesConfig.ACTIVE_POLICY_STATE)
                    .setType(PoliciesConfig.METADATA_POLICY_TYPE)
                    .setActors(new DataHubActorFilter().setResourceOwners(true))
                    .setPrivileges(
                        new StringArray(List.of(PoliciesConfig.VIEW_ENTITY_PRIVILEGE.getType())))));
    assertEquals(
        ESAccessControlUtil.buildAccessControlFilters(ownerNoGroupsNoType),
        Optional.of(
            QueryBuilders.boolQuery()
                .should(
                    QueryBuilders.termsQuery(
                        "owners.keyword",
                        List.of(
                            TEST_USER_A.toString().toLowerCase(),
                            TEST_GROUP_A.toString().toLowerCase(),
                            TEST_GROUP_C.toString().toLowerCase())))
                .minimumShouldMatch(1)),
        "Expected user filter for owners without group filter");

    OperationContext ownerWithGroupsNoType =
        sessionWithPolicy(
            Set.of(
                new DataHubPolicyInfo()
                    .setState(PoliciesConfig.ACTIVE_POLICY_STATE)
                    .setType(PoliciesConfig.METADATA_POLICY_TYPE)
                    .setActors(
                        new DataHubActorFilter()
                            .setResourceOwners(true)
                            .setGroups(new UrnArray(TEST_GROUP_A)))
                    .setPrivileges(
                        new StringArray(List.of(PoliciesConfig.VIEW_ENTITY_PRIVILEGE.getType())))));
    assertEquals(
        ESAccessControlUtil.buildAccessControlFilters(ownerWithGroupsNoType),
        Optional.of(
            QueryBuilders.boolQuery()
                .should(
                    QueryBuilders.termsQuery(
                        "owners.keyword",
                        List.of(
                            TEST_USER_A.toString().toLowerCase(),
                            TEST_GROUP_A.toString().toLowerCase(),
                            TEST_GROUP_C.toString().toLowerCase())))
                .minimumShouldMatch(1)),
        "Expected user AND group filter for owners");
  }

  @Test
  public void testUserGroupOwnerTypes() {
    OperationContext ownerTypeBusinessNoUserNoGroup =
        sessionWithPolicy(
            Set.of(
                new DataHubPolicyInfo()
                    .setState(PoliciesConfig.ACTIVE_POLICY_STATE)
                    .setType(PoliciesConfig.METADATA_POLICY_TYPE)
                    .setActors(
                        new DataHubActorFilter().setResourceOwnersTypes(new UrnArray(BUS_OWNER)))
                    .setPrivileges(
                        new StringArray(List.of(PoliciesConfig.VIEW_ENTITY_PRIVILEGE.getType())))));
    assertEquals(
        ESAccessControlUtil.buildAccessControlFilters(ownerTypeBusinessNoUserNoGroup),
        Optional.of(
            QueryBuilders.boolQuery()
                .should(
                    QueryBuilders.boolQuery()
                        .should(
                            QueryBuilders.termsQuery(
                                "ownerTypes."
                                    + OwnerTypeMap.encodeFieldName(BUS_OWNER.toString())
                                    + ".keyword",
                                List.of(
                                    TEST_USER_A.toString().toLowerCase(),
                                    TEST_GROUP_A.toString().toLowerCase(),
                                    TEST_GROUP_C.toString().toLowerCase())))
                        .minimumShouldMatch(1))
                .minimumShouldMatch(1)),
        "Expected user filter for business owner via user or group urn");

    OperationContext ownerTypeBusinessMultiUserNoGroup =
        sessionWithPolicy(
            Set.of(
                new DataHubPolicyInfo()
                    .setState(PoliciesConfig.ACTIVE_POLICY_STATE)
                    .setType(PoliciesConfig.METADATA_POLICY_TYPE)
                    .setActors(
                        new DataHubActorFilter()
                            .setResourceOwnersTypes(new UrnArray(BUS_OWNER))
                            .setUsers(new UrnArray(List.of(TEST_USER_A, TEST_USER_B))))
                    .setPrivileges(
                        new StringArray(List.of(PoliciesConfig.VIEW_ENTITY_PRIVILEGE.getType())))));
    assertEquals(
        ESAccessControlUtil.buildAccessControlFilters(ownerTypeBusinessMultiUserNoGroup),
        Optional.of(
            QueryBuilders.boolQuery()
                .should(
                    QueryBuilders.boolQuery()
                        .should(
                            QueryBuilders.termsQuery(
                                "ownerTypes."
                                    + OwnerTypeMap.encodeFieldName(BUS_OWNER.toString())
                                    + ".keyword",
                                List.of(
                                    TEST_USER_A.toString().toLowerCase(),
                                    TEST_GROUP_A.toString().toLowerCase(),
                                    TEST_GROUP_C.toString().toLowerCase())))
                        .minimumShouldMatch(1))
                .minimumShouldMatch(1)),
        "Expected user filter for `business owner` by owner user/group A urn (excluding other user/group B)");

    OperationContext ownerWithGroupsBusTechMultiGroup =
        sessionWithPolicy(
            Set.of(
                new DataHubPolicyInfo()
                    .setState(PoliciesConfig.ACTIVE_POLICY_STATE)
                    .setType(PoliciesConfig.METADATA_POLICY_TYPE)
                    .setActors(
                        new DataHubActorFilter()
                            .setResourceOwnersTypes(new UrnArray(BUS_OWNER, TECH_OWNER))
                            .setGroups(new UrnArray(TEST_GROUP_A, TEST_GROUP_B)))
                    .setPrivileges(
                        new StringArray(List.of(PoliciesConfig.VIEW_ENTITY_PRIVILEGE.getType())))));
    assertEquals(
        ESAccessControlUtil.buildAccessControlFilters(ownerWithGroupsBusTechMultiGroup),
        Optional.of(
            QueryBuilders.boolQuery()
                .should(
                    QueryBuilders.boolQuery()
                        .should(
                            QueryBuilders.termsQuery(
                                "ownerTypes."
                                    + OwnerTypeMap.encodeFieldName(BUS_OWNER.toString())
                                    + ".keyword",
                                List.of(
                                    TEST_USER_A.toString().toLowerCase(),
                                    TEST_GROUP_A.toString().toLowerCase(),
                                    TEST_GROUP_C.toString().toLowerCase())))
                        .should(
                            QueryBuilders.termsQuery(
                                "ownerTypes."
                                    + OwnerTypeMap.encodeFieldName(TECH_OWNER.toString())
                                    + ".keyword",
                                List.of(
                                    TEST_USER_A.toString().toLowerCase(),
                                    TEST_GROUP_A.toString().toLowerCase(),
                                    TEST_GROUP_C.toString().toLowerCase())))
                        .minimumShouldMatch(1))
                .minimumShouldMatch(1)),
        "Expected filter for business owner or technical owner by group A (excluding other group B and owner privilege)");

    OperationContext ownerWithMultiUserMultiGroupsBusTech =
        sessionWithPolicy(
            Set.of(
                new DataHubPolicyInfo()
                    .setState(PoliciesConfig.ACTIVE_POLICY_STATE)
                    .setType(PoliciesConfig.METADATA_POLICY_TYPE)
                    .setActors(
                        new DataHubActorFilter()
                            .setResourceOwnersTypes(new UrnArray(BUS_OWNER, TECH_OWNER))
                            .setUsers(new UrnArray(List.of(TEST_USER_A, TEST_USER_B)))
                            .setGroups(new UrnArray(TEST_GROUP_A, TEST_GROUP_B)))
                    .setPrivileges(
                        new StringArray(List.of(PoliciesConfig.VIEW_ENTITY_PRIVILEGE.getType())))));
    assertEquals(
        ESAccessControlUtil.buildAccessControlFilters(ownerWithMultiUserMultiGroupsBusTech),
        Optional.of(
            QueryBuilders.boolQuery()
                .should(
                    QueryBuilders.boolQuery()
                        .should(
                            QueryBuilders.termsQuery(
                                "ownerTypes."
                                    + OwnerTypeMap.encodeFieldName(BUS_OWNER.toString())
                                    + ".keyword",
                                List.of(
                                    TEST_USER_A.toString().toLowerCase(),
                                    TEST_GROUP_A.toString().toLowerCase(),
                                    TEST_GROUP_C.toString().toLowerCase())))
                        .should(
                            QueryBuilders.termsQuery(
                                "ownerTypes."
                                    + OwnerTypeMap.encodeFieldName(TECH_OWNER.toString())
                                    + ".keyword",
                                List.of(
                                    TEST_USER_A.toString().toLowerCase(),
                                    TEST_GROUP_A.toString().toLowerCase(),
                                    TEST_GROUP_C.toString().toLowerCase())))
                        .minimumShouldMatch(1))
                .minimumShouldMatch(1)),
        "Expected filter for business owner or technical owner by user A and group A (excluding other group B and owner privilege)");
  }

  private static OperationContext sessionWithPolicy(Set<DataHubPolicyInfo> policies) {
    return sessionWithPolicy(policies, List.of(TEST_GROUP_A, TEST_GROUP_C));
  }

  private static OperationContext sessionWithPolicy(
      Set<DataHubPolicyInfo> policies, List<Urn> groups) {
    Authorizer mockAuthorizer = mock(Authorizer.class);
    when(mockAuthorizer.getActorPolicies(eq(UrnUtils.getUrn(USER_AUTH.getActor().toUrnStr()))))
        .thenReturn(policies);
    when(mockAuthorizer.getActorGroups(eq(UrnUtils.getUrn(USER_AUTH.getActor().toUrnStr()))))
        .thenReturn(groups);

    return ENABLED_CONTEXT.asSession(mockAuthorizer, USER_AUTH);
  }
}
