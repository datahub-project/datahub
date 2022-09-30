package com.datahub.plugins.auth.authentication;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nonnull;


/**
 *  Context class to provide Authenticator implementations with concrete objects necessary for their correct workings.
 *  DataHub creates {@link AuthenticatorContext} instance and pass it as an argument to init method of {@link Authenticator}
 */
public class AuthenticatorContext {
  private final Map<String, Object> contextMap;

  public AuthenticatorContext(@Nonnull final Map<String, Object> context) {
    Objects.requireNonNull(context);
    contextMap = new HashMap<>();
    contextMap.putAll(context);
  }

  /**
   *
   * @return contextMap
   * The contextMap contains below key and value
   *  PLUGIN_DIRECTORY: <Plugin directory where plugin jar is installed>
   */
  @Nonnull
  public Map<String, Object> data() {
    return contextMap;
  }
}
