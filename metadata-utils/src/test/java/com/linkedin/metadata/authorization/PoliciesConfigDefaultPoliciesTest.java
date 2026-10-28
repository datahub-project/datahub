package com.linkedin.metadata.authorization;

import static com.linkedin.metadata.authorization.ApiGroup.ENTITY;
import static com.linkedin.metadata.authorization.PoliciesConfig.GET_ENTITY_PRIVILEGE;
import static com.linkedin.metadata.authorization.PoliciesConfig.VIEW_ENTITY_PAGE_PRIVILEGE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.policy.DataHubPolicyInfo;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.testng.annotations.Test;

/**
 * The default policies are granted to every actor on their own entity without an administrator
 * opting in, so any privilege they carry beyond read access is a self-service escalation: edit
 * access on your own corpuser entity is enough to grant yourself the Admin role.
 */
public class PoliciesConfigDefaultPoliciesTest {

  private static final Urn ACTOR_URN = UrnUtils.getUrn("urn:li:corpuser:test");

  @Test
  public void testDefaultPoliciesGrantOnlyEntityReadPrivileges() {
    assertEquals(
        grantedPrivilegeTypes(),
        Set.of(VIEW_ENTITY_PAGE_PRIVILEGE.getType(), GET_ENTITY_PRIVILEGE.getType()),
        "Default self policies must remain read-only");
  }

  @Test
  public void testDefaultPoliciesDoNotSatisfyMutatingEntityOperations() {
    final Set<String> granted = grantedPrivilegeTypes();

    assertTrue(
        satisfies(granted, ApiOperation.READ),
        "Default self policies must still allow read on self");

    for (ApiOperation operation :
        List.of(
            ApiOperation.CREATE, ApiOperation.UPDATE, ApiOperation.DELETE, ApiOperation.EXECUTE)) {
      assertFalse(
          satisfies(granted, operation),
          String.format("Default self policies must not authorize ENTITY %s on self", operation));
    }
  }

  private static Set<String> grantedPrivilegeTypes() {
    return PoliciesConfig.getDefaultPolicies(ACTOR_URN).stream()
        .map(DataHubPolicyInfo::getPrivileges)
        .flatMap(List::stream)
        .collect(Collectors.toSet());
  }

  /**
   * Mirrors {@code AuthUtil.isAPIAuthorized}: an operation is authorized once the actor holds every
   * privilege of at least one of the disjoint conjunctives required for it.
   */
  private static boolean satisfies(Set<String> granted, ApiOperation operation) {
    final Disjunctive<Conjunctive<PoliciesConfig.Privilege>> required =
        PoliciesConfig.API_PRIVILEGE_MAP.get(ENTITY).get(operation);
    return required.stream()
        .anyMatch(
            conjunctive ->
                !conjunctive.isEmpty()
                    && granted.containsAll(
                        conjunctive.stream()
                            .map(PoliciesConfig.Privilege::getType)
                            .collect(Collectors.toSet())));
  }
}
