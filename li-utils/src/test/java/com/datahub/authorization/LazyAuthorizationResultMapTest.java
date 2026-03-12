package com.datahub.authorization;

import static org.testng.Assert.*;

import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.testng.annotations.Test;

public class LazyAuthorizationResultMapTest {
  @Test
  public void testContainsReturnsTrueOnlyForAvailablePrivileges() {
    var p1 = "p1";
    var p2 = "p2";
    LazyAuthorizationResultMap results =
        new LazyAuthorizationResultMap(
            Set.of(p1, p2),
            privilege -> new AuthorizationResult(null, AuthorizationResult.Type.DENY, ""));

    assertTrue(results.containsKey(p1));
    assertTrue(results.containsKey(p2));
    assertFalse(results.containsKey("other"));
  }

  @Test
  public void testGetComputesOnlyOneTime() {
    var p1 = "p1";
    var callsCount = new AtomicInteger(0);
    LazyAuthorizationResultMap results =
        new LazyAuthorizationResultMap(
            Set.of(p1),
            privilege -> {
              callsCount.incrementAndGet();
              return new AuthorizationResult(null, AuthorizationResult.Type.DENY, "");
            });

    assertSame(results.get(p1), results.get(p1));
    assertEquals(callsCount.get(), 1);
  }
}
