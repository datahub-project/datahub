package com.linkedin.datahub.graphql.resolvers.policy;

import com.datahub.metadata.authorization.AuthorizationRequest;
import com.datahub.metadata.authorization.AuthorizationResult;
import com.datahub.metadata.authorization.Authorizer;
import com.google.common.collect.ImmutableList;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.metadata.authorization.PoliciesConfig;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nonnull;


public class PolicyAuthUtils {

  static boolean canManagePolicies(@Nonnull QueryContext context) {
    final Authorizer authorizer = context.getAuthorizer();
    final String principal = context.getActor();
    return isAuthorized(principal, ImmutableList.of(PoliciesConfig.MANAGE_POLICIES_PRIVILEGE.getType()), authorizer);
  }

  static boolean isAuthorized(
      String principal,
      List<String> privilegeGroup,
      Authorizer authorizer) {
    for (final String privilege : privilegeGroup) {
      final AuthorizationRequest request = new AuthorizationRequest(principal, privilege, Optional.empty());
      final AuthorizationResult result = authorizer.authorize(request);
      if (AuthorizationResult.Type.DENY.equals(result.getType())) {
        return false;
      }
    }
    return true;
  }

  private PolicyAuthUtils() { }
}
