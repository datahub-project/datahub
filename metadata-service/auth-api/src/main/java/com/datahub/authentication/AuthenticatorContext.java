package com.datahub.authentication;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nonnull;


/**
 *  Context class to provide Authenticator implementations with concrete objects necessary for their correct workings.
 */
public class AuthenticatorContext {

  private final Map<String, Object> contextMap;

  public AuthenticatorContext(@Nonnull final Map<String, Object> context) {
    Objects.requireNonNull(context);
    contextMap = new HashMap<>();
    contextMap.putAll(context);
  }

  @Nonnull
  public Map<String, Object> data() {
    return contextMap;
  }
}
