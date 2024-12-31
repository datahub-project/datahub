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
