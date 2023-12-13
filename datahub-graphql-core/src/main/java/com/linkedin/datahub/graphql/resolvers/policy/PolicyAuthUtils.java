package com.linkedin.datahub.graphql.resolvers.policy;

import static com.linkedin.datahub.graphql.resolvers.AuthUtils.*;

import com.datahub.plugins.auth.authorization.Authorizer;
import com.google.common.collect.ImmutableList;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.metadata.authorization.PoliciesConfig;
import javax.annotation.Nonnull;

public class PolicyAuthUtils {

  static boolean canManagePolicies(@Nonnull QueryContext context) {
    final Authorizer authorizer = context.getAuthorizer();
    final String principal = context.getActorUrn();
    return isAuthorized(
        principal,
        ImmutableList.of(PoliciesConfig.MANAGE_POLICIES_PRIVILEGE.getType()),
        authorizer);
  }

  private PolicyAuthUtils() {}
}
