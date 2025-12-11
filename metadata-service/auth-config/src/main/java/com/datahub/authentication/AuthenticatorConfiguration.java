/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.datahub.authentication;

import java.util.Map;
import lombok.Data;

/**
 * POJO representing {@link com.datahub.plugins.auth.authentication.Authenticator} configurations
 * provided in the application.yaml.
 */
@Data
public class AuthenticatorConfiguration {
  /**
   * A fully-qualified class name for the {@link
   * com.datahub.plugins.auth.authentication.Authenticator} implementation to be registered.
   */
  private String type;

  /**
   * A set of authenticator-specific configurations passed through during "init" of the
   * authenticator.
   */
  private Map<String, Object> configs;
}
