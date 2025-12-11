/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.datahub.graphql.resolvers.connection;

import com.datahub.authorization.AuthUtil;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.metadata.authorization.PoliciesConfig;
import javax.annotation.Nonnull;

/** Utilities for working with DataHub Connections. */
public class ConnectionUtils {

  /**
   * Returns true if the user is able to read and or write connection between DataHub and external
   * platforms.
   */
  public static boolean canManageConnections(@Nonnull QueryContext context) {
    return AuthUtil.isAuthorized(
        context.getOperationContext(), PoliciesConfig.MANAGE_CONNECTIONS_PRIVILEGE);
  }

  private ConnectionUtils() {}
}
