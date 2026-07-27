package com.datahub.authorization;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.RecordTemplate;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.testng.annotations.Test;

public class AuthorizationRequestTest {

  @Test
  public void fiveArgConstructorSetsAllFields() {
    EntitySpec resource = new EntitySpec("dataset", "urn:li:dataset:test");
    Map<String, List<RecordTemplate>> actorPolicies =
        Map.of("VIEW", Collections.<RecordTemplate>emptyList());

    AuthorizationRequest request =
        new AuthorizationRequest(
            "urn:li:corpuser:testuser",
            "VIEW",
            Optional.of(resource),
            List.of(resource),
            actorPolicies);

    assertEquals(request.getActorUrn(), "urn:li:corpuser:testuser");
    assertEquals(request.getPrivilege(), "VIEW");
    assertEquals(request.getResourceSpec(), Optional.of(resource));
    assertEquals(request.getSubResources(), List.of(resource));
    assertEquals(request.getActorPoliciesByPrivilege(), actorPolicies);
  }

  @Test
  public void sevenArgConstructorSetsSessionActorFields() {
    EntitySpec resource = new EntitySpec("dataset", "urn:li:dataset:test");
    SessionActorIdentity identity =
        SessionActorIdentity.empty(UrnUtils.getUrn("urn:li:corpuser:testuser"));

    AuthorizationRequest request =
        new AuthorizationRequest(
            "urn:li:corpuser:testuser",
            "VIEW",
            Optional.of(resource),
            List.of(resource),
            null,
            List.of(UrnUtils.getUrn("urn:li:corpGroup:test")),
            Set.of(UrnUtils.getUrn("urn:li:dataHubRole:Admin")),
            identity);

    assertEquals(
        request.getActorGroupMembership(), List.of(UrnUtils.getUrn("urn:li:corpGroup:test")));
    assertEquals(
        request.getActorDirectRoles(), Set.of(UrnUtils.getUrn("urn:li:dataHubRole:Admin")));
    assertEquals(request.getSessionActorIdentity(), identity);
  }

  @Test
  public void sixArgConstructorLeavesSessionActorIdentityNull() {
    AuthorizationRequest request =
        new AuthorizationRequest(
            "urn:li:corpuser:testuser",
            "VIEW",
            Optional.empty(),
            Collections.emptyList(),
            null,
            Collections.emptyList(),
            Set.of());

    assertNull(request.getSessionActorIdentity());
  }

  @Test
  public void fourArgConstructorLeavesActorPoliciesByPrivilegeNull() {
    AuthorizationRequest request =
        new AuthorizationRequest(
            "urn:li:corpuser:testuser", "VIEW", Optional.empty(), Collections.emptyList());

    assertNull(request.getActorPoliciesByPrivilege());
  }
}
