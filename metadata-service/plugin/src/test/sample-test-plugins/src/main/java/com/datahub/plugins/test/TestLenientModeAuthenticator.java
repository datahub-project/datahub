package com.datahub.plugins.test;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationException;
import com.datahub.authentication.AuthenticationRequest;
import com.datahub.authentication.AuthenticatorContext;
import com.datahub.plugins.auth.authentication.Authenticator;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class TestLenientModeAuthenticator implements Authenticator {
  @Override
  public void init(
      @Nonnull Map<String, Object> authenticatorConfig, @Nullable AuthenticatorContext context) {}

  @Nullable
  @Override
  public Authentication authenticate(@Nonnull AuthenticationRequest authenticationRequest)
      throws AuthenticationException {
    // We should be able to access user directory as we are going to be loaded with Lenient mode
    // IsolatedClassLoader
    String userHome = System.getProperty("user.home");
    assert userHome != null;
    return new Authentication(new Actor(ActorType.USER, "fake"), "foo:bar");
  }
}
