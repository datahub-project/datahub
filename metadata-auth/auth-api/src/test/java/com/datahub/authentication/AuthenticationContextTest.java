package com.datahub.authentication;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

public class AuthenticationContextTest {

  @AfterMethod
  public void cleanup() {
    AuthenticationContext.remove();
  }

  @Test
  public void testMaybeAuthenticationEmptyWhenUnset() {
    assertTrue(AuthenticationContext.maybeAuthentication().isEmpty());
    assertTrue(AuthenticationContext.maybeActorUrn().isEmpty());
  }

  @Test
  public void testMaybeAuthenticationPresent() {
    Authentication auth =
        new Authentication(new Actor(ActorType.USER, "datahub"), "credentials");
    AuthenticationContext.setAuthentication(auth);

    assertEquals(AuthenticationContext.maybeAuthentication().orElseThrow(), auth);
    assertEquals(AuthenticationContext.maybeActorUrn().orElseThrow(), auth.getActor().toUrnStr());
  }
}
