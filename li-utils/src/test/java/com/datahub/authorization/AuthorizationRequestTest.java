package com.datahub.authorization;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

import com.linkedin.data.template.RecordTemplate;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
  public void fourArgConstructorLeavesActorPoliciesByPrivilegeNull() {
    AuthorizationRequest request =
        new AuthorizationRequest(
            "urn:li:corpuser:testuser", "VIEW", Optional.empty(), Collections.emptyList());

    assertNull(request.getActorPoliciesByPrivilege());
  }
}
