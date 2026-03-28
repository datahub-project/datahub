package io.datahubproject.test.metadata.context;

import static org.testng.Assert.*;

import com.datahub.authorization.AuthorizationResult;
import com.datahub.authorization.BatchAuthorizationResult;
import com.datahub.authorization.EntitySpec;
import java.util.List;
import java.util.Set;
import org.testng.annotations.Test;

public class TestAuthSessionTest {
  @Test
  public void denyAllDeniesRequests() {
    BatchAuthorizationResult result =
        TestAuthSession.DENY_ALL.authorize(Set.of("p1", "p2"), null, List.of());
    assertEquals(result.getResults().get("p1").getType(), AuthorizationResult.Type.DENY);
    assertEquals(result.getResults().get("p2").getType(), AuthorizationResult.Type.DENY);
  }

  @Test
  public void denyAllReturnsNullForUnexpectedPrivilege() {
    BatchAuthorizationResult result =
        TestAuthSession.DENY_ALL.authorize(Set.of("p1"), null, List.of());
    assertNull(result.getResults().get("p2"));
  }

  @Test
  public void allowOnlyBehavesCorrectly() {
    EntitySpec entitySpec = new EntitySpec("entityType", "urn:li:entityType:id1");
    BatchAuthorizationResult result =
        TestAuthSession.allowOnly(entitySpec, Set.of("p1"))
            .authorize(Set.of("p1", "p2", "p3"), entitySpec, List.of());
    assertEquals(result.getResults().get("p1").getType(), AuthorizationResult.Type.ALLOW);
    assertEquals(result.getResults().get("p2").getType(), AuthorizationResult.Type.DENY);
  }

  @Test
  public void allowOnlyReturnsNullForUnexpectedPrivilege() {
    EntitySpec entitySpec = new EntitySpec("entityType", "urn:li:entityType:id1");
    BatchAuthorizationResult result =
        TestAuthSession.allowOnly(entitySpec, Set.of("p1"))
            .authorize(Set.of("p1"), entitySpec, List.of());
    assertNull(result.getResults().get("p2"));
  }

  @Test
  public void allowAnyForBehavesCorrectly() {
    EntitySpec entitySpec = new EntitySpec("entityType", "urn:li:entityType:id1");
    BatchAuthorizationResult result =
        TestAuthSession.allowAnyFor(entitySpec)
            .authorize(Set.of("p1", "p2", "p3"), entitySpec, List.of());
    assertEquals(result.getResults().get("p1").getType(), AuthorizationResult.Type.ALLOW);
    assertEquals(result.getResults().get("p2").getType(), AuthorizationResult.Type.ALLOW);
    assertEquals(result.getResults().get("p3").getType(), AuthorizationResult.Type.ALLOW);
  }

  @Test
  public void allowAnyForReturnsNullForUnexpectedPrivilege() {
    EntitySpec entitySpec = new EntitySpec("entityType", "urn:li:entityType:id1");
    BatchAuthorizationResult result =
        TestAuthSession.allowAnyFor(entitySpec).authorize(Set.of("p1"), entitySpec, List.of());
    assertNull(result.getResults().get("p2"));
  }
}
