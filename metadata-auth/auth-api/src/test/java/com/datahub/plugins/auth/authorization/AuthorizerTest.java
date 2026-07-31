package com.datahub.plugins.auth.authorization;

import static org.testng.Assert.assertFalse;

import com.linkedin.common.urn.UrnUtils;
import org.testng.annotations.Test;

public class AuthorizerTest {

  @Test
  public void defaultResolveSessionActorIdentityReturnsEmpty() {
    assertFalse(
        Authorizer.EMPTY
            .resolveSessionActorIdentity(UrnUtils.getUrn("urn:li:corpuser:test"))
            .isPresent());
  }
}
