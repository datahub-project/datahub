/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.datahub.authorization;

import com.datahub.authorization.config.ViewAuthorizationConfiguration;
import com.datahub.plugins.auth.authorization.Authorizer;
import java.util.List;
import lombok.Data;

/** POJO representing the "authentication" configuration block in application.yaml. */
@Data
public class AuthorizationConfiguration {
  /** Configuration for the default DataHub Policies-based authorizer. */
  private DefaultAuthorizerConfiguration defaultAuthorizer;

  /** List of configurations for {@link Authorizer}s to be registered */
  private List<AuthorizerConfiguration> authorizers;

  private ViewAuthorizationConfiguration view;
}
