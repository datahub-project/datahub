/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.datahub.graphql.resolvers.policy;

import static com.linkedin.metadata.Constants.POLICY_ENTITY_NAME;
import static com.linkedin.metadata.authorization.ApiOperation.MANAGE;

import com.datahub.authorization.AuthUtil;
import com.linkedin.datahub.graphql.QueryContext;
import java.util.List;
import javax.annotation.Nonnull;

public class PolicyAuthUtils {

  static boolean canManagePolicies(@Nonnull QueryContext context) {
    return AuthUtil.isAuthorizedEntityType(
        context.getOperationContext(), MANAGE, List.of(POLICY_ENTITY_NAME));
  }

  private PolicyAuthUtils() {}
}
