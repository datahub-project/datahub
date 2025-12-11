/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.datahub.authorization;

import com.datahub.authentication.AuthenticatorContext;
import com.datahub.plugins.auth.authentication.Authenticator;
import java.util.Map;
import javax.annotation.Nonnull;
import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * Context provided to an Authorizer on initialization. DataHub creates {@link AuthenticatorContext}
 * instance and provides it as an argument to init method of {@link Authenticator}
 */
@Data
@AllArgsConstructor
public class AuthorizerContext {
  private final Map<String, Object> contextMap;

  /** A utility for resolving an {@link EntitySpec} to resolved entity field values. */
  private EntitySpecResolver entitySpecResolver;

  /**
   * @return contextMap The contextMap contains below key and value PLUGIN_DIRECTORY: Directory path
   *     where plugin is installed i.e. PLUGIN_HOME
   */
  @Nonnull
  public Map<String, Object> data() {
    return contextMap;
  }
}
