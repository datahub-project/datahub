/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.datahub.authorization;

import com.datahub.plugins.auth.authorization.Authorizer;
import java.util.Map;
import lombok.Data;

/** POJO representing {@link Authorizer} configurations provided in the application.yaml. */
@Data
public class AuthorizerConfiguration {
  /** Whether to enable this authorizer */
  private boolean enabled;

  /** A fully-qualified class name for the {@link Authorizer} implementation to be registered. */
  private String type;

  /** A set of authorizer-specific configurations passed through during "init" of the authorizer. */
  private Map<String, Object> configs;
}
