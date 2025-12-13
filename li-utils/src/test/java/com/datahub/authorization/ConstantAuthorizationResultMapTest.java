package com.datahub.authorization;

import static org.testng.Assert.*;

import java.util.Random;
import org.apache.commons.lang3.RandomStringUtils;
import org.testng.annotations.Test;

public class ConstantAuthorizationResultMapTest {

  private final Random _random = new Random();

  @Test
  public void testContainsAlwaysReturnsTrue() {
    ConstantAuthorizationResultMap results =
        new ConstantAuthorizationResultMap(AuthorizationResult.Type.DENY);

    for (int i = 0; i < 20; i++) {
      String randomPrivilege = RandomStringUtils.random(10, 0, 0, true, true, null, _random);
      assertTrue(results.containsKey(randomPrivilege));
    }
  }

  @Test
  public void testGetReturnConstantResult() {
    ConstantAuthorizationResultMap results =
        new ConstantAuthorizationResultMap(AuthorizationResult.Type.DENY);

    AuthorizationResult authorizationResult = results.get("abc");
    for (int i = 0; i < 20; i++) {
      String randomPrivilege = RandomStringUtils.random(10, 0, 0, true, true, null, new Random());
      assertSame(results.get(randomPrivilege), authorizationResult);
    }
  }
}
