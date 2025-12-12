package com.datahub.authorization;

import static org.testng.Assert.*;

import java.util.Set;
import org.testng.annotations.Test;

public class PredefinedAuthorizationResultMapTest {
  @Test
  public void testContainsReturnsTrueAllPrivileges() {
    var p1 = "p1";
    PredefinedAuthorizationResultMap results = new PredefinedAuthorizationResultMap(Set.of(p1));

    assertTrue(results.containsKey(p1));
    assertTrue(results.containsKey("other"));
  }

  @Test
  public void testGetReturnAllowOnlyForPredefinedPrivileges() {
    var p1 = "p1";
    var p2 = "p2";
    PredefinedAuthorizationResultMap results = new PredefinedAuthorizationResultMap(Set.of(p1, p2));

    assertEquals(results.get(p1).getType(), AuthorizationResult.Type.ALLOW);
    assertEquals(results.get(p2).getType(), AuthorizationResult.Type.ALLOW);
    assertEquals(results.get("other").getType(), AuthorizationResult.Type.DENY);
  }
}
