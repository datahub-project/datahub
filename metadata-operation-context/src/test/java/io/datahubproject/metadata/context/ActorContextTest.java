package io.datahubproject.metadata.context;

import static com.linkedin.metadata.Constants.CORP_USER_STATUS_SUSPENDED;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.datahub.context.OperationFingerprint;
import com.linkedin.common.Status;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.StringArray;
import com.linkedin.entity.Aspect;
import com.linkedin.identity.CorpUserStatus;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.metadata.key.CorpUserKey;
import com.linkedin.policy.DataHubActorFilter;
import com.linkedin.policy.DataHubPolicyInfo;
import com.linkedin.policy.DataHubResourceFilter;
import com.linkedin.policy.PolicyMatchCondition;
import com.linkedin.policy.PolicyMatchCriterion;
import com.linkedin.policy.PolicyMatchCriterionArray;
import com.linkedin.policy.PolicyMatchFilter;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.testng.annotations.Test;

public class ActorContextTest {

  private static final DataHubPolicyInfo POLICY_ABC =
      new DataHubPolicyInfo()
          .setState(PoliciesConfig.ACTIVE_POLICY_STATE)
          .setActors(
              new DataHubActorFilter()
                  .setUsers(
                      new UrnArray(
                          UrnUtils.getUrn("urn:li:corpUser:userA"),
                          UrnUtils.getUrn("urn:li:corpUser:userB"))))
          .setPrivileges(new StringArray(List.of("a", "b", "c")));

  private static final DataHubPolicyInfo POLICY_D =
      new DataHubPolicyInfo()
          .setState(PoliciesConfig.ACTIVE_POLICY_STATE)
          .setActors(
              new DataHubActorFilter()
                  .setUsers(
                      new UrnArray(
                          UrnUtils.getUrn("urn:li:corpUser:userA"),
                          UrnUtils.getUrn("urn:li:corpUser:userB"))))
          .setPrivileges(new StringArray(List.of("d")));

  private static final DataHubPolicyInfo POLICY_ABC_RESOURCE =
      new DataHubPolicyInfo()
          .setState(PoliciesConfig.ACTIVE_POLICY_STATE)
          .setActors(
              new DataHubActorFilter()
                  .setUsers(
                      new UrnArray(
                          UrnUtils.getUrn("urn:li:corpUser:userA"),
                          UrnUtils.getUrn("urn:li:corpUser:userB"))))
          .setResources(
              new DataHubResourceFilter()
                  .setFilter(
                      new PolicyMatchFilter()
                          .setCriteria(
                              new PolicyMatchCriterionArray(
                                  List.of(
                                      new PolicyMatchCriterion()
                                          .setField("tag")
                                          .setCondition(PolicyMatchCondition.EQUALS)
                                          .setValues(new StringArray("urn:li:tag:test")))))))
          .setPrivileges(new StringArray(List.of("a", "b", "c")));

  private static final DataHubPolicyInfo POLICY_D_OWNER =
      new DataHubPolicyInfo()
          .setState(PoliciesConfig.ACTIVE_POLICY_STATE)
          .setActors(new DataHubActorFilter().setResourceOwners(true))
          .setPrivileges(new StringArray(List.of("d")));

  private static final DataHubPolicyInfo POLICY_D_OWNER_TYPE =
      new DataHubPolicyInfo()
          .setState(PoliciesConfig.ACTIVE_POLICY_STATE)
          .setActors(
              new DataHubActorFilter()
                  .setResourceOwnersTypes(
                      new UrnArray(UrnUtils.getUrn("urn:li:ownershipType:test"))))
          .setPrivileges(new StringArray(List.of("d")));

  @Test
  public void actorContextId() {
    Authentication userAuth = new Authentication(new Actor(ActorType.USER, "USER"), "");

    assertEquals(
        ActorContext.asSessionRestricted(userAuth, Set.of(), Set.of(), Set.of(), true)
            .getCacheKeyComponent(),
        ActorContext.asSessionRestricted(userAuth, Set.of(), Set.of(), Set.of(), true)
            .getCacheKeyComponent(),
        "Expected equality across instances");

    assertEquals(
        ActorContext.asSessionRestricted(userAuth, Set.of(), Set.of(), Set.of(), true)
            .getCacheKeyComponent(),
        ActorContext.asSessionRestricted(
                userAuth,
                Set.of(),
                Set.of(UrnUtils.getUrn("urn:li:corpGroup:group1")),
                Set.of(),
                true)
            .getCacheKeyComponent(),
        "Expected no impact to cache context from group membership");

    assertEquals(
        ActorContext.asSessionRestricted(
                userAuth, Set.of(POLICY_ABC, POLICY_D), Set.of(), Set.of(), true)
            .getCacheKeyComponent(),
        ActorContext.asSessionRestricted(
                userAuth, Set.of(POLICY_ABC, POLICY_D), Set.of(), Set.of(), true)
            .getCacheKeyComponent(),
        "Expected equality when non-ownership policies are identical");

    assertNotEquals(
        ActorContext.asSessionRestricted(
                userAuth, Set.of(POLICY_ABC_RESOURCE, POLICY_D), Set.of(), Set.of(), true)
            .getCacheKeyComponent(),
        ActorContext.asSessionRestricted(
                userAuth, Set.of(POLICY_ABC, POLICY_D), Set.of(), Set.of(), true)
            .getCacheKeyComponent(),
        "Expected differences with non-identical resource policy");

    assertNotEquals(
        ActorContext.asSessionRestricted(userAuth, Set.of(POLICY_D_OWNER), Set.of(), Set.of(), true)
            .getCacheKeyComponent(),
        ActorContext.asSessionRestricted(userAuth, Set.of(POLICY_D), Set.of(), Set.of(), true)
            .getCacheKeyComponent(),
        "Expected differences with ownership policy");

    assertNotEquals(
        ActorContext.asSessionRestricted(
                userAuth, Set.of(POLICY_D_OWNER_TYPE), Set.of(), Set.of(), true)
            .getCacheKeyComponent(),
        ActorContext.asSessionRestricted(userAuth, Set.of(POLICY_D), Set.of(), Set.of(), true)
            .getCacheKeyComponent(),
        "Expected differences with ownership type policy");
  }

  @Test
  public void indexPoliciesByPrivilegeGroupsPoliciesByPrivilege() {
    Map<String, List<RecordTemplate>> indexed =
        ActorContext.indexPoliciesByPrivilege(Set.of(POLICY_ABC, POLICY_D));

    assertEquals(indexed.get("a"), List.of(POLICY_ABC));
    assertEquals(indexed.get("b"), List.of(POLICY_ABC));
    assertEquals(indexed.get("c"), List.of(POLICY_ABC));
    assertEquals(indexed.get("d"), List.of(POLICY_D));
    assertEquals(ActorContext.indexPoliciesByPrivilege(Set.of()).size(), 0);
    assertEquals(ActorContext.indexPoliciesByPrivilege(null).size(), 0);
  }

  @Test
  public void indexPoliciesByPrivilegeSkipsPoliciesWithNullPrivileges() {
    DataHubPolicyInfo policyWithoutPrivileges = mock(DataHubPolicyInfo.class);
    when(policyWithoutPrivileges.getPrivileges()).thenReturn(null);
    Set<DataHubPolicyInfo> policies = new HashSet<>();
    policies.add(POLICY_ABC);
    policies.add(policyWithoutPrivileges);

    Map<String, List<RecordTemplate>> indexed = ActorContext.indexPoliciesByPrivilege(policies);

    assertEquals(indexed.get("a"), List.of(POLICY_ABC));
    assertEquals(indexed.size(), 3);
  }

  @Test
  public void isSystemSessionMatchesSystemAuthentication() {
    Authentication systemAuth =
        new Authentication(new Actor(ActorType.USER, "__datahub_system"), "");
    Authentication userAuth = new Authentication(new Actor(ActorType.USER, "user"), "");

    assertTrue(ActorContext.isSystemSession(systemAuth, systemAuth));
    assertFalse(ActorContext.isSystemSession(userAuth, systemAuth));
    assertFalse(ActorContext.isSystemSession(userAuth, null));
  }

  @Test
  public void isActiveSkipsLookupForSystemActor() {
    Authentication systemAuth =
        new Authentication(new Actor(ActorType.USER, "__datahub_system"), "");
    ActorContext ctx =
        ActorContext.asSessionRestricted(systemAuth, Set.of(), List.of(), Set.of(), true);
    AspectRetriever retriever = mock(AspectRetriever.class);
    assertTrue(ctx.isActive(mock(OperationContext.class), retriever));
    verifyNoInteractions(retriever);
  }

  @Test
  public void isActiveFalseWhenEnforcingExistenceAndCorpUserKeyMissing() {
    Urn userUrn = UrnUtils.getUrn("urn:li:corpuser:nobody");
    Authentication userAuth = new Authentication(new Actor(ActorType.USER, "nobody"), "");
    ActorContext ctx =
        ActorContext.asSessionRestricted(userAuth, Set.of(), List.of(), Set.of(), true);
    AspectRetriever retriever = mock(AspectRetriever.class);
    when(retriever.getLatestAspectObjects(any(OperationFingerprint.class), any(), any()))
        .thenReturn(
            Map.of(userUrn, Map.of("status", new Aspect(new Status().setRemoved(false).data()))));
    assertFalse(ctx.isActive(mock(OperationContext.class), retriever));
  }

  @Test
  public void isActiveTrueWhenNotEnforcingExistenceAndCorpUserKeyMissing() {
    Urn userUrn = UrnUtils.getUrn("urn:li:corpuser:nobody");
    Authentication userAuth = new Authentication(new Actor(ActorType.USER, "nobody"), "");
    ActorContext ctx =
        ActorContext.asSessionRestricted(userAuth, Set.of(), List.of(), Set.of(), false);
    AspectRetriever retriever = mock(AspectRetriever.class);
    when(retriever.getLatestAspectObjects(any(OperationFingerprint.class), any(), any()))
        .thenReturn(Map.of(userUrn, Map.of()));
    assertTrue(ctx.isActive(mock(OperationContext.class), retriever));
  }

  @Test
  public void isActiveTrueWithCorpUserKeyAndNoFlags() {
    Urn userUrn = UrnUtils.getUrn("urn:li:corpuser:activeone");
    Authentication userAuth = new Authentication(new Actor(ActorType.USER, "activeone"), "");
    ActorContext ctx =
        ActorContext.asSessionRestricted(userAuth, Set.of(), List.of(), Set.of(), true);
    CorpUserKey key = new CorpUserKey().setUsername("activeone");
    AspectRetriever retriever = mock(AspectRetriever.class);
    when(retriever.getLatestAspectObjects(any(OperationFingerprint.class), any(), any()))
        .thenReturn(
            Map.of(
                userUrn,
                Map.of(
                    "corpUserKey", new Aspect(key.data()),
                    "status", new Aspect(new Status().setRemoved(false).data()),
                    "corpUserStatus",
                        new Aspect(new CorpUserStatus().setStatus("ACTIVE").data()))));
    assertTrue(ctx.isActive(mock(OperationContext.class), retriever));
  }

  @Test
  public void isActiveFalseWhenCorpUserSuspended() {
    Urn userUrn = UrnUtils.getUrn("urn:li:corpuser:suspended");
    Authentication userAuth = new Authentication(new Actor(ActorType.USER, "suspended"), "");
    ActorContext ctx =
        ActorContext.asSessionRestricted(userAuth, Set.of(), List.of(), Set.of(), true);
    CorpUserKey key = new CorpUserKey().setUsername("suspended");
    CorpUserStatus suspended = new CorpUserStatus().setStatus(CORP_USER_STATUS_SUSPENDED);
    AspectRetriever retriever = mock(AspectRetriever.class);
    when(retriever.getLatestAspectObjects(any(OperationFingerprint.class), any(), any()))
        .thenReturn(
            Map.of(
                userUrn,
                Map.of(
                    "corpUserKey", new Aspect(key.data()),
                    "status", new Aspect(new Status().setRemoved(false).data()),
                    "corpUserStatus", new Aspect(suspended.data()))));
    assertFalse(ctx.isActive(mock(OperationContext.class), retriever));
  }
}
