package com.datahub.plugins.test;

import com.datahub.plugins.auth.authentication.Actor;
import com.datahub.plugins.auth.authentication.ActorType;
import com.datahub.plugins.auth.authentication.Authentication;
import com.datahub.plugins.auth.authentication.AuthenticationException;
import com.datahub.plugins.auth.authentication.AuthenticationRequest;
import com.datahub.plugins.auth.authentication.Authenticator;
import com.datahub.plugins.auth.authentication.AuthenticatorContext;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;


public class TestLenientModeAuthenticator implements Authenticator {
  @Override
  public void init(@Nonnull Map<String, Object> authenticatorConfig, @Nullable AuthenticatorContext context) {

  }

  @Nullable
  @Override
  public Authentication authenticate(@Nonnull AuthenticationRequest authenticationRequest)
      throws AuthenticationException {
    // We should be able to access user directory as we are going to be loaded with Lenient mode IsolatedClassLoader
    String userHome = System.getProperty("user.home");
    assert userHome != null;
    return new Authentication(new Actor(ActorType.USER, "fake"), "foo:bar");
  }
}
