/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.datahub.authentication;

import java.util.List;
import lombok.Data;

/** POJO representing the "authentication" configuration block in application.yaml. */
@Data
public class AuthenticationConfiguration {
  /** Whether authentication is enabled */
  private boolean enabled;

  /** Whether user existence is enforced */
  private boolean enforceExistenceEnabled;

  /** Paths to be excluded from filtering * */
  private String excludedPaths;

  /**
   * List of configurations for {@link com.datahub.plugins.auth.authentication.Authenticator}s to be
   * registered
   */
  private List<AuthenticatorConfiguration> authenticators;

  /** Unique id to identify internal system callers */
  private String systemClientId;

  /** Unique secret to authenticate internal system callers */
  private String systemClientSecret;

  /** The lifespan of a UI session token. */
  private long sessionTokenDurationMs;

  private TokenServiceConfiguration tokenService;
}
