package com.linkedin.datahub.graphql.resolvers.policy;

import static com.datahub.authorization.AuthUtil.isAuthorized;

import com.datahub.plugins.auth.authorization.Authorizer;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.authorization.ApiOperation;
import com.linkedin.metadata.authorization.PoliciesConfig;
import javax.annotation.Nonnull;

public class PolicyAuthUtils {

  static boolean canManagePolicies(@Nonnull QueryContext context) {
    final Authorizer authorizer = context.getAuthorizer();
    final String principal = context.getActorUrn();
    return isAuthorized(
        authorizer,
        principal,
        PoliciesConfig.lookupEntityAPIPrivilege(Constants.POLICY_ENTITY_NAME, ApiOperation.MANAGE));
  }

  private PolicyAuthUtils() {}
}
