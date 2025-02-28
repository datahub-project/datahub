package io.datahubproject.metadata.context;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringArray;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.policy.DataHubActorFilter;
import com.linkedin.policy.DataHubPolicyInfo;
import com.linkedin.policy.DataHubResourceFilter;
import com.linkedin.policy.PolicyMatchCondition;
import com.linkedin.policy.PolicyMatchCriterion;
import com.linkedin.policy.PolicyMatchCriterionArray;
import com.linkedin.policy.PolicyMatchFilter;
import java.util.List;
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
        ActorContext.asSessionRestricted(userAuth, Set.of(), Set.of(), true).getCacheKeyComponent(),
        ActorContext.asSessionRestricted(userAuth, Set.of(), Set.of(), true).getCacheKeyComponent(),
        "Expected equality across instances");

    assertEquals(
        ActorContext.asSessionRestricted(userAuth, Set.of(), Set.of(), true).getCacheKeyComponent(),
        ActorContext.asSessionRestricted(
                userAuth, Set.of(), Set.of(UrnUtils.getUrn("urn:li:corpGroup:group1")), true)
            .getCacheKeyComponent(),
        "Expected no impact to cache context from group membership");

    assertEquals(
        ActorContext.asSessionRestricted(userAuth, Set.of(POLICY_ABC, POLICY_D), Set.of(), true)
            .getCacheKeyComponent(),
        ActorContext.asSessionRestricted(userAuth, Set.of(POLICY_ABC, POLICY_D), Set.of(), true)
            .getCacheKeyComponent(),
        "Expected equality when non-ownership policies are identical");

    assertNotEquals(
        ActorContext.asSessionRestricted(
                userAuth, Set.of(POLICY_ABC_RESOURCE, POLICY_D), Set.of(), true)
            .getCacheKeyComponent(),
        ActorContext.asSessionRestricted(userAuth, Set.of(POLICY_ABC, POLICY_D), Set.of(), true)
            .getCacheKeyComponent(),
        "Expected differences with non-identical resource policy");

    assertNotEquals(
        ActorContext.asSessionRestricted(userAuth, Set.of(POLICY_D_OWNER), Set.of(), true)
            .getCacheKeyComponent(),
        ActorContext.asSessionRestricted(userAuth, Set.of(POLICY_D), Set.of(), true)
            .getCacheKeyComponent(),
        "Expected differences with ownership policy");

    assertNotEquals(
        ActorContext.asSessionRestricted(userAuth, Set.of(POLICY_D_OWNER_TYPE), Set.of(), true)
            .getCacheKeyComponent(),
        ActorContext.asSessionRestricted(userAuth, Set.of(POLICY_D), Set.of(), true)
            .getCacheKeyComponent(),
        "Expected differences with ownership type policy");
  }
}
