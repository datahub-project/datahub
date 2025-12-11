/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.datahub.authentication;

import com.datahub.plugins.auth.authentication.Authenticator;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nonnull;

/**
 * Context class to provide Authenticator implementations with concrete objects necessary for their
 * correct workings. DataHub creates {@link AuthenticatorContext} instance and provides it as an
 * argument to init method of {@link Authenticator}
 */
public class AuthenticatorContext {
  private final Map<String, Object> contextMap;

  public AuthenticatorContext(@Nonnull final Map<String, Object> context) {
    Objects.requireNonNull(context);
    contextMap = new HashMap<>();
    contextMap.putAll(context);
  }

  /**
   * @return contextMap The contextMap contains below key and value {@link
   *     com.datahub.plugins.PluginConstant#PLUGIN_HOME PLUGIN_HOME}: Directory path where plugin is
   *     installed
   */
  @Nonnull
  public Map<String, Object> data() {
    return contextMap;
  }
}
